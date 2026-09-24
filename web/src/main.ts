/** Browser-only historical train observer. */

import { MapboxOverlay } from "@deck.gl/mapbox";
import { PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import maplibregl from "maplibre-gl";
import "@fontsource-variable/noto-sans";
import "maplibre-gl/dist/maplibre-gl.css";
import "./style.css";

import { SWITZERLAND_BORDER } from "./assets/switzerland-border";
import { Clock, SPEEDS } from "./clock";
import { BUFFER_SECONDS, INITIAL_VIEW, MAP_STYLE_URL, TERRAIN_TILE_URL, datasetUrl } from "./config";
import { fetchRoutes, type RoutePath, type Routes } from "./routes";
import {
  type ColorMode,
  delayColor,
  isJourney,
  type JourneyRef,
  positioned,
  PUNCTUAL_S,
  trainsLayer,
  type PositionedLeg,
  typeColors,
} from "./render/trains";
import { type DatasetInfo, FLAG_SCHEDULED_FALLBACK, type Leg, type Manifest } from "./types";
import type {
  JourneySearchResult,
  StationBoardDeparture,
  WorkerRequestPayload,
  WorkerResponse,
} from "./worker/legs.worker";

type RGB = [number, number, number];

interface Station {
  id: number;
  name: string;
  lon: number;
  lat: number;
  /** Station ids are dataset-local; a Finnish and a Swiss station may share a number. */
  dataset: number;
}

interface TrackPath extends RoutePath {
  dataset: number;
}

/** One country's static layer: geometry, stations, and dictionaries, all dataset-local. */
interface DatasetView {
  info: DatasetInfo;
  manifest: Manifest;
  routes: Routes;
  stations: Station[];
  stationById: Map<number, Station>;
  stationDepartureRouteIds: Map<number, number[]>;
  routePairs: Record<string, [number, number]>;
  types: string[];
  /** Service-group id per type id, resolved once against this dataset's own codes. */
  groupOfType: string[];
  colors: RGB[];
  trackPaths: TrackPath[];
}

interface BergE2ETestHook {
  highlightedStationRouteIds: () => number[];
  openStation: (name: string) => boolean;
  stationPoints: () => Array<Station & { x: number; y: number }>;
  trainPoints: () => Array<{ dataset: string; journeyId: number; x: number; y: number }>;
  loadedDatasets: () => string[];
  selectedJourneyId: () => number | null;
  selectedRouteIds: () => number[];
}

declare global {
  interface Window {
    __BERG_E2E__?: BergE2ETestHook;
  }
}

interface ServiceGroup {
  id: string;
  label: string;
  /** Train type codes per dataset. Codes are not comparable across countries: "S" is an
   * S-Bahn in Switzerland and a Pendolino in Finland. */
  codes: Record<string, ReadonlySet<string>>;
}

const SERVICE_GROUPS: ServiceGroup[] = [
  {
    id: "s",
    label: "S-Bahn",
    codes: { ch: new Set(["S", "SN"]), fi: new Set(["HL", "HLV"]) },
  },
  {
    id: "regional",
    label: "Regional",
    codes: {
      ch: new Set(["R", "RB", "RE", "IRE", "TER", "PE"]),
      fi: new Set(["H", "HDM", "HSM"]),
    },
  },
  {
    id: "intercity",
    label: "IC / IR",
    codes: { ch: new Set(["IC", "IR"]), fi: new Set(["IC", "IC2", "P", "PVV", "PVS"]) },
  },
  {
    id: "fast",
    label: "ICE / fast",
    codes: { ch: new Set(["ICE", "TGV", "EC", "RJ", "RJX"]), fi: new Set(["S", "AE"]) },
  },
  { id: "night", label: "Night", codes: { ch: new Set(["NJ", "EN", "NZ"]), fi: new Set(["PYO"]) } },
];
const OTHER_GROUP = "other";

const REFETCH_MARGIN_S = 60;
const DEFAULT_DAY = "2018-01-01";
let nextRequestId = 1;

/** Approximate ground resolution at the map center for zoom-adaptive arrow smoothing. */
const metersPerPixel = (latitude: number, zoom: number): number =>
  (156_543.03392 * Math.cos((latitude * Math.PI) / 180)) / 2 ** zoom;

/** route_id is uint16 within a dataset, so this packs (dataset, route) into one map key. */
const trackKey = (dataset: number, routeId: number): number => dataset * 65_536 + routeId;

const byId = <T extends HTMLElement>(id: string): T => {
  const element = document.getElementById(id);
  if (!element) throw new Error(`missing #${id}`);
  return element as T;
};

const escapeHtml = (value: string): string =>
  value.replace(/[&<>'"]/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[char]!);

const fmtClock = (epoch: number, timeZone: string): string =>
  new Date(epoch * 1000).toLocaleString("de-CH", {
    timeZone,
    weekday: "short",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

const fmtShortTime = (epoch: number, timeZone: string): string =>
  new Date(epoch * 1000).toLocaleTimeString("de-CH", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
  });

const fmtHudDate = (epoch: number, timeZone: string): string =>
  new Date(epoch * 1000).toLocaleDateString("de-CH", {
    timeZone,
    weekday: "short",
    day: "2-digit",
    month: "long",
    year: "numeric",
  });

const fmtHudTime = (epoch: number, timeZone: string): string =>
  new Date(epoch * 1000).toLocaleTimeString("de-CH", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

const utcDay = (epoch: number): string => new Date(Math.floor(epoch) * 1000).toISOString().slice(0, 10);

/** The public train number inside a dataset's journey identity. */
const trainNumber = (tripId: string, datasetId: string): string => {
  if (datasetId !== "ch") return tripId.split(" ").at(-1) ?? tripId;
  const parts = tripId.split(":");
  if (tripId.includes(":sjyid:")) return parts[parts.length - 1]?.split("-")[0] ?? tripId;
  return parts.length >= 3 ? parts[parts.length - 2]! : tripId;
};

/** Convert a wall-clock value in `timeZone` without depending on the viewer's own zone. */
function zonedEpoch(value: string, timeZone: string): number | null {
  const match = /^(\d{4}-\d{2}-\d{2})(?:[ T](\d{1,2}):(\d{2}))?$/.exec(value.trim());
  if (!match) return null;
  const hour = Number(match[2] ?? 8);
  const minute = Number(match[3] ?? 0);
  if (hour > 23 || minute > 59) return null;
  const desiredAsUtc = Date.parse(`${match[1]}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00Z`);
  if (!Number.isFinite(desiredAsUtc)) return null;
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(new Date(desiredAsUtc)).map((part) => [part.type, part.value]),
  );
  const renderedAsUtc = Date.parse(
    `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:00Z`,
  );
  return (desiredAsUtc - (renderedAsUtc - desiredAsUtc)) / 1000;
}

function ask(worker: Worker, payload: WorkerRequestPayload): Promise<WorkerResponse> {
  const requestId = nextRequestId++;
  return new Promise((resolve, reject) => {
    const onMessage = (event: MessageEvent<WorkerResponse>) => {
      if (event.data.requestId !== requestId) return;
      worker.removeEventListener("message", onMessage);
      if (event.data.kind === "error") reject(new Error(event.data.message));
      else resolve(event.data);
    };
    worker.addEventListener("message", onMessage);
    worker.postMessage({ ...payload, requestId });
  });
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${url}`);
  return (await response.json()) as T;
}

/** Everything static for one dataset, fetched once when the country first comes into view. */
async function loadDatasetView(info: DatasetInfo, manifest: Manifest): Promise<DatasetView> {
  const [routes, allStations, typeMap, routePairs] = await Promise.all([
    fetchRoutes(datasetUrl(info.path, "static/routes.bin")),
    fetchJson<Array<Omit<Station, "dataset">>>(datasetUrl(info.path, "static/stations.json")),
    fetchJson<Record<string, string>>(datasetUrl(info.path, "static/train_types.json")),
    fetchJson<Record<string, [number, number]>>(datasetUrl(info.path, "static/route_pairs.json")),
  ]);

  const served = new Set<number>();
  const stationDepartureRouteIds = new Map<number, number[]>();
  for (const [routeIdText, [from, to]] of Object.entries(routePairs)) {
    const routeId = Number(routeIdText);
    served.add(from);
    served.add(to);
    stationDepartureRouteIds.set(from, [...(stationDepartureRouteIds.get(from) ?? []), routeId]);
  }
  const stations: Station[] = allStations
    .filter((station) => served.has(station.id))
    .map(({ id, name, lon, lat }) => ({ id, name, lon, lat, dataset: info.index }));

  const maxType = Math.max(0, ...Object.keys(typeMap).map(Number));
  const types = Array.from({ length: maxType + 1 }, (_, index) => typeMap[String(index)] ?? "?");
  const groupOfType = types.map(
    (code) => SERVICE_GROUPS.find((group) => group.codes[info.id]?.has(code))?.id ?? OTHER_GROUP,
  );
  // Switzerland keeps its original per-code palette. Other countries colour by service group,
  // because their codes mean different things.
  const [local, regional, longDistance] = typeColors(["S", "RE", "IC"]) as [RGB, RGB, RGB];
  const colors: RGB[] =
    info.id === "ch"
      ? typeColors(types)
      : groupOfType.map((group) => (group === "s" ? local : group === "regional" ? regional : longDistance));

  return {
    info,
    manifest,
    routes,
    stations,
    stationById: new Map(stations.map((station) => [station.id, station])),
    stationDepartureRouteIds,
    routePairs,
    types,
    groupOfType,
    colors,
    trackPaths: routes.paths().map((route) => ({ ...route, dataset: info.index })),
  };
}

async function main(): Promise<void> {
  const e2eMode =
    import.meta.env.MODE === "e2e" ||
    (import.meta.env.DEV && new URLSearchParams(window.location.search).has("e2e"));
  const loading = byId<HTMLDivElement>("loading");
  const loadingLabel = byId<HTMLSpanElement>("loading-label");
  const loadingProgress = byId<HTMLElement>("loading-progress");
  const setProgress = (percent: number, label: string) => {
    loadingProgress.style.width = `${percent}%`;
    loadingLabel.textContent = label;
  };

  setProgress(8, "Starting the in-browser database…");
  const worker = new Worker(new URL("./worker/legs.worker.ts", import.meta.url), { type: "module" });
  const ready = await ask(worker, { kind: "init" });
  if (ready.kind !== "ready") throw new Error("worker did not become ready");
  const datasets: DatasetInfo[] = ready.datasets;
  const manifests: Manifest[] = ready.manifests;
  const multiCountry = datasets.length > 1;
  for (const manifest of manifests) {
    if (!manifest.start || !manifest.end) throw new Error("a manifest advertises no days");
  }

  setProgress(24, "Loading routes, stations, and train classes…");
  const views: Array<DatasetView | undefined> = new Array(datasets.length);
  const viewLoads = new Map<number, Promise<DatasetView | undefined>>();
  // Bumped whenever a country's static layer arrives, so the map rebuilds its static layers.
  let staticVersion = 0;
  const ensureView = (index: number): Promise<DatasetView | undefined> => {
    let load = viewLoads.get(index);
    if (!load) {
      load = loadDatasetView(datasets[index]!, manifests[index]!).then(
        (view) => {
          views[index] = view;
          staticVersion++;
          return view;
        },
        (error: unknown) => {
          // One country's missing geometry must not take the others down; retry on next view.
          console.warn(`static layer for ${datasets[index]!.id} unavailable`, error);
          viewLoads.delete(index);
          return undefined;
        },
      );
      viewLoads.set(index, load);
    }
    return load;
  };
  // Switzerland is dataset 0 and the initial view; other countries load when panned into view.
  const swiss = await ensureView(0);
  if (!swiss) throw new Error("the Swiss static layer could not be loaded");
  const loadedViews = (): DatasetView[] => views.filter((view): view is DatasetView => view !== undefined);

  setProgress(48, "Preparing the observed rail network…");
  const enabledGroups = new Set([...SERVICE_GROUPS.map((group) => group.id), OTHER_GROUP]);
  const allGroups = [...SERVICE_GROUPS.map(({ id, label }) => ({ id, label })), { id: OTHER_GROUP, label: "Other" }];

  const typeEnabled = (dataset: number, typeId: number): boolean =>
    enabledGroups.has(views[dataset]?.groupOfType[typeId] ?? OTHER_GROUP);
  const typeName = (dataset: number, typeId: number): string => views[dataset]?.types[typeId] ?? "Train";
  const typeColor = (leg: Leg): RGB => views[leg.dataset]?.colors[leg.type] ?? [200, 200, 200];

  setProgress(64, "Loading the dark map and mountain relief…");
  const map = new maplibregl.Map({
    container: "map",
    style: MAP_STYLE_URL,
    center: [INITIAL_VIEW.longitude, INITIAL_VIEW.latitude],
    zoom: INITIAL_VIEW.zoom,
    pitch: e2eMode ? 0 : 24,
    bearing: 0,
    maxPitch: 70,
    attributionControl: false,
  });
  await map.once("load");

  if (!e2eMode) {
    try {
      map.addSource("berg-terrain", {
        type: "raster-dem",
        tiles: [TERRAIN_TILE_URL],
        tileSize: 256,
        maxzoom: 15,
        encoding: "terrarium",
        attribution: '<a href="https://github.com/tilezen/joerd/blob/master/docs/attribution.md">Terrain data sources</a>',
      });
      const firstLabel = map.getStyle().layers?.find((layer) => layer.type === "symbol")?.id;
      map.addLayer(
        {
          id: "berg-hillshade",
          type: "hillshade",
          source: "berg-terrain",
          paint: {
            "hillshade-method": "multidirectional",
            "hillshade-exaggeration": 0.28,
            "hillshade-shadow-color": "#020509",
            "hillshade-highlight-color": "#607184",
            "hillshade-accent-color": "#111b25",
          },
        },
        firstLabel,
      );
    } catch (error) {
      console.warn("terrain relief unavailable", error);
    }
  }

  const overlay = new MapboxOverlay({
    interleaved: false,
    layers: [],
    getCursor: ({ isDragging, isHovering }) =>
      isDragging ? "grabbing" : isHovering ? "pointer" : "grab",
  });
  map.addControl(overlay);

  /** Countries whose bounding box overlaps the visible map, in dataset order. */
  const visibleDatasets = (): number[] => {
    const bounds = map.getBounds();
    return datasets
      .filter(({ bbox: [west, south, east, north] }) =>
        west <= bounds.getEast() && east >= bounds.getWest() && south <= bounds.getNorth() && north >= bounds.getSouth(),
      )
      .map((info) => info.index);
  };
  /** The country the clock speaks for: the one under the map center, else Switzerland. */
  const focusDataset = (): DatasetInfo => {
    const { lng, lat } = map.getCenter();
    return (
      datasets.find(({ bbox: [west, south, east, north] }) => lng >= west && lng <= east && lat >= south && lat <= north) ??
      datasets[0]!
    );
  };

  const swissDays = Object.keys(manifests[0]!.days).sort();
  const allDays = [...new Set(manifests.flatMap((manifest) => Object.keys(manifest.days)))].sort();
  const substantialDays = swissDays.filter((day) => manifests[0]!.days[day]!.legs >= 10_000);
  const initialDay = DEFAULT_DAY in manifests[0]!.days
    ? DEFAULT_DAY
    : (substantialDays[substantialDays.length - 1] ?? swissDays[swissDays.length - 1]!);
  const tMin = Date.parse(`${allDays[0]}T00:00:00Z`) / 1000;
  const tMax = Date.parse(`${allDays[allDays.length - 1]}T23:59:59Z`) / 1000;
  const clock = new Clock(Date.parse(`${initialDay}T06:00:00Z`) / 1000);
  clock.setSpeed(600);
  clock.play();

  const topbar = byId<HTMLElement>("topbar");
  const filters = byId<HTMLElement>("filters");
  const hudDateElement = byId<HTMLSpanElement>("hud-date");
  const timeElement = byId<HTMLTimeElement>("time");
  const countElement = byId<HTMLSpanElement>("count");
  const coverageElement = byId<HTMLDivElement>("coverage");
  const speedBadge = byId<HTMLButtonElement>("speed-badge");
  const speedValue = byId<HTMLElement>("speed-value");
  const playbackButton = byId<HTMLButtonElement>("playback-button");
  const playbackIcon = byId<HTMLSpanElement>("playback-icon");
  const playbackLabel = byId<HTMLElement>("playback-label");
  const filterButton = byId<HTMLButtonElement>("filter-button");
  const filterSummary = byId<HTMLElement>("filter-summary");
  const archiveMeta = byId<HTMLDivElement>("archive-meta");
  const dataSources = byId<HTMLDivElement>("data-sources");
  const details = byId<HTMLElement>("details");
  const detailsContent = byId<HTMLDivElement>("details-content");
  const command = byId<HTMLDivElement>("command");
  const commandInput = byId<HTMLInputElement>("command-input");
  const commandContext = byId<HTMLDivElement>("command-context");
  const commandResults = byId<HTMLDivElement>("command-results");
  const filterChips = byId<HTMLDivElement>("filter-chips");
  const legendElement = byId<HTMLDivElement>("legend");
  let colorMode: ColorMode = "type";
  let visibleStationBoard: { station: Station; departures: StationBoardDeparture[] } | null = null;
  let stationBoardGeneration = 0;

  const setFiltersOpen = (open: boolean) => {
    filters.classList.toggle("hidden", !open);
    filterButton.setAttribute("aria-expanded", String(open));
  };
  filterButton.onclick = () => setFiltersOpen(filters.classList.contains("hidden"));
  byId<HTMLButtonElement>("filters-close").onclick = () => setFiltersOpen(false);

  const togglePlayback = () => {
    if (clock.paused) clock.play();
    else clock.pause();
  };
  playbackButton.onclick = togglePlayback;

  const updateFilterSummary = () => {
    const enabled = enabledGroups.size;
    const services = enabled === allGroups.length ? "All services" : `${enabled} of ${allGroups.length} services`;
    filterSummary.textContent = `${services} · ${colorMode === "type" ? "service" : "delay"} colours`;
  };

  archiveMeta.textContent = multiCountry
    ? datasets
        .map((info, index) => `${info.name} ${Object.keys(manifests[index]!.days).length.toLocaleString()} days`)
        .join(" · ")
    : `${swissDays.length.toLocaleString()} days · ${swiss.routes.length.toLocaleString()} routes · ${swiss.stations.length.toLocaleString()} stations`;
  // Every dataset keeps its own attribution and licence, shown wherever its data is.
  const sourceLink = (info: DatasetInfo): string =>
    `${escapeHtml(info.attribution)} · <a href="${escapeHtml(info.license_url)}" target="_blank" rel="noreferrer">${escapeHtml(info.license)}</a>`;
  dataSources.innerHTML = datasets.map(sourceLink).join("<br>");

  for (const group of allGroups) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "chip on";
    button.textContent = group.label;
    button.onclick = () => {
      if (enabledGroups.has(group.id)) enabledGroups.delete(group.id);
      else enabledGroups.add(group.id);
      button.classList.toggle("on", enabledGroups.has(group.id));
      updateFilterSummary();
      if (visibleStationBoard) {
        renderStationBoard(visibleStationBoard.station, visibleStationBoard.departures);
      }
    };
    filterChips.appendChild(button);
  }

  const swatch = (color: RGB, label: string): string =>
    `<span class="key"><i style="background:rgb(${color.join(",")})"></i>${label}</span>`;
  const modeButtons: Record<ColorMode, HTMLElement> = {
    type: byId("mode-type"),
    delay: byId("mode-delay"),
  };
  const renderLegend = () => {
    const sample = (delay: number, flags = 0): Leg => ({
      dataset: 0,
      route_id: 0,
      journey_id: 0,
      route_start: 0,
      route_end: 1,
      t_dep: 0,
      dur: 1,
      type: 0,
      delay,
      flags,
    });
    legendElement.innerHTML =
      colorMode === "delay"
        ? [
            swatch(delayColor(sample(0)), "On time"),
            swatch(delayColor(sample(PUNCTUAL_S)), "3 minutes"),
            swatch(delayColor(sample(600)), "10 minutes"),
            swatch(delayColor(sample(1800)), "30+ minutes"),
            swatch(delayColor(sample(0, FLAG_SCHEDULED_FALLBACK)), "Unmeasured"),
          ].join("")
        : [
            swatch(typeColors(["S"])[0]!, "S-Bahn"),
            swatch(typeColors(["RE"])[0]!, "Regional"),
            swatch(typeColors(["IC"])[0]!, "Long-distance"),
          ].join("");
  };
  for (const mode of ["type", "delay"] as ColorMode[]) {
    modeButtons[mode].onclick = () => {
      colorMode = mode;
      for (const [key, element] of Object.entries(modeButtons)) {
        element.classList.toggle("on", key === mode);
      }
      renderLegend();
      updateFilterSummary();
    };
  }
  renderLegend();
  updateFilterSummary();

  const trackPathById = new Map<number, TrackPath>();
  let trackLayer: PathLayer<TrackPath> | null = null;
  let stationLayer: ScatterplotLayer<Station> | null = null;
  let allStations: Station[] = [];
  let builtStaticVersion = -1;
  /** Rebuild the static layers only when a country's static layer has arrived. */
  const refreshStaticLayers = () => {
    if (builtStaticVersion === staticVersion) return;
    builtStaticVersion = staticVersion;
    const loaded = loadedViews();
    const paths = loaded.flatMap((view) => view.trackPaths);
    trackPathById.clear();
    for (const route of paths) trackPathById.set(trackKey(route.dataset, route.routeId), route);
    allStations = loaded.flatMap((view) => view.stations);
    trackLayer = new PathLayer<TrackPath>({
      id: "observed-rail-network",
      data: paths,
      getPath: (route) => route.path,
      getColor: (route) => (route.fallback ? [92, 102, 116, 30] : [105, 124, 143, 85]),
      getWidth: 1,
      widthUnits: "pixels",
      widthMinPixels: 0.65,
      pickable: false,
    });
    stationLayer = new ScatterplotLayer<Station>({
      id: "stations",
      data: allStations,
      getPosition: (station) => [station.lon, station.lat],
      getFillColor: [177, 190, 205, 165],
      getLineColor: [7, 11, 17, 220],
      // A geographic radius naturally grows on screen as the user zooms in. Pixel clamps keep
      // stations usable at national zoom without letting them dominate close-up views.
      getRadius: 80,
      radiusUnits: "meters",
      radiusMinPixels: 2.5,
      radiusMaxPixels: 15,
      stroked: true,
      lineWidthMinPixels: 1,
      pickable: true,
      autoHighlight: true,
      highlightColor: [255, 0, 0, 220],
      onClick: ({ object }) => {
        if (object) void openStation(object);
      },
    });
  };
  const countryBorderLayer = new PathLayer<{ path: [number, number][] }>({
    id: "switzerland-border",
    data: [{ path: SWITZERLAND_BORDER }],
    getPath: ({ path }) => path,
    getColor: [255, 255, 255, 95],
    getWidth: 1,
    widthUnits: "pixels",
    widthMinPixels: 0.65,
    widthMaxPixels: 1,
    capRounded: true,
    jointRounded: true,
    pickable: false,
  });

  let selectedJourney: JourneySearchResult | null = null;
  let selectedJourneyTracks: TrackPath[] = [];
  let highlightedStationRoutes: { dataset: number; routeIds: number[] } | null = null;
  let spectateGeneration = 0;
  const selectedRef = (): JourneyRef | null =>
    selectedJourney ? { dataset: selectedJourney.dataset, journeyId: selectedJourney.journeyId } : null;

  const showDetails = (html: string) => {
    detailsContent.innerHTML = html;
    details.classList.remove("hidden");
  };
  const hideDetails = () => details.classList.add("hidden");
  const stopSpectating = (hidePanel = true) => {
    spectateGeneration++;
    selectedJourney = null;
    selectedJourneyTracks = [];
    if (hidePanel) hideDetails();
  };
  const stationSpectateAction = () =>
    selectedJourney
      ? `<div class="watch-actions"><button class="primary" data-stop-spectating data-keep-details type="button">Stop spectating</button></div>`
      : "";
  /** Provenance and evidence semantics for a detail panel. */
  const sourceNote = (dataset: number): string => {
    const info = datasets[dataset]!;
    return `<div class="source">${escapeHtml(info.name)} · ${escapeHtml(info.time_semantics)} times · ${sourceLink(info)}</div>`;
  };
  detailsContent.addEventListener("click", (event) => {
    const target = event.target instanceof Element
      ? event.target.closest<HTMLButtonElement>("[data-stop-spectating]")
      : null;
    if (!target) return;
    const keepDetails = target.hasAttribute("data-keep-details");
    stopSpectating(!keepDetails);
    if (keepDetails) target.closest(".watch-actions")?.remove();
  });
  // Closing a panel must never leave an invisible camera-follow session behind.
  byId<HTMLButtonElement>("details-close").onclick = () => {
    stationBoardGeneration++;
    visibleStationBoard = null;
    highlightedStationRoutes = null;
    stopSpectating();
  };

  let e2ePositionedTrains: PositionedLeg[] = [];
  if (e2eMode) {
    window.__BERG_E2E__ = {
      highlightedStationRouteIds: () => [...(highlightedStationRoutes?.routeIds ?? [])],
      openStation: (name) => {
        const station = allStations.find((candidate) => candidate.name === name);
        if (!station) return false;
        void openStation(station);
        return true;
      },
      stationPoints: () =>
        allStations.map((station) => {
          const point = map.project([station.lon, station.lat]);
          return { ...station, x: point.x, y: point.y };
        }),
      trainPoints: () =>
        e2ePositionedTrains.map((train) => {
          const point = map.project(train.pos);
          return {
            dataset: datasets[train.leg.dataset]!.id,
            journeyId: train.leg.journey_id,
            x: point.x,
            y: point.y,
          };
        }),
      loadedDatasets: () => loadedViews().map((view) => view.info.id),
      selectedJourneyId: () => selectedJourney?.journeyId ?? null,
      selectedRouteIds: () => selectedJourneyTracks.map((route) => route.routeId),
    };
  }

  function stationName(dataset: number, id: number): string {
    return views[dataset]?.stationById.get(id)?.name ?? `Station ${id}`;
  }

  function routeDescription(dataset: number, routeId: number): { from: string; to: string } {
    const pair = views[dataset]?.routePairs[String(routeId)];
    return pair
      ? { from: stationName(dataset, pair[0]), to: stationName(dataset, pair[1]) }
      : { from: "Unknown origin", to: "Unknown destination" };
  }

  async function openStation(station: Station): Promise<void> {
    const generation = ++stationBoardGeneration;
    visibleStationBoard = null;
    highlightedStationRoutes = null;
    showDetails(`
      <div class="eyebrow">Station</div>
      <h2>${escapeHtml(station.name)}</h2>
      <div class="sub">Loading observed departures…</div>
      ${stationSpectateAction()}`);
    map.easeTo({ center: [station.lon, station.lat], zoom: Math.max(map.getZoom(), 11), duration: 650 });
    try {
      const response = await ask(worker, {
        kind: "station-board",
        dataset: station.dataset,
        routeIds: views[station.dataset]?.stationDepartureRouteIds.get(station.id) ?? [],
        simTime: clock.simTime,
        horizon: 3 * 3600,
      });
      if (generation !== stationBoardGeneration || response.kind !== "station-board") return;
      visibleStationBoard = { station, departures: response.departures };
      renderStationBoard(station, response.departures);
    } catch (error) {
      if (generation !== stationBoardGeneration) return;
      highlightedStationRoutes = null;
      showDetails(`
        <div class="eyebrow">Station</div><h2>${escapeHtml(station.name)}</h2>
        <div class="empty">Could not load the board: ${escapeHtml(String(error))}</div>
        ${stationSpectateAction()}`);
    }
  }

  function renderStationBoard(station: Station, departures: StationBoardDeparture[]): void {
    highlightedStationRoutes = null;
    const dataset = station.dataset;
    const timeZone = datasets[dataset]!.timezone;
    const manifest = manifests[dataset]!;
    const filteredDepartures = departures.filter((departure) => typeEnabled(dataset, departure.type));
    const visible = filteredDepartures.slice(0, 36);
    const renderedDepartures: StationBoardDeparture[] = [];
    const covered = utcDay(clock.simTime) in manifest.days;
    const body = visible.length
      ? visible
          .map((departure) => {
            const stops: number[] = [];
            for (const routeId of departure.routeIds) {
              const pair = views[dataset]?.routePairs[String(routeId)];
              if (!pair || pair[0] === pair[1]) continue;
              if (stops[stops.length - 1] !== pair[1]) stops.push(pair[1]);
            }
            const destination = stops.at(-1);
            if (destination === undefined) return "";
            const departureIndex = renderedDepartures.push(departure) - 1;
            const via = stops.slice(0, -1).map((stop) => stationName(dataset, stop));
            const viaSummary = via.length
              ? `via ${escapeHtml(via[0]!)}${via.length > 1 ? ` +${via.length - 1}` : ""}`
              : "direct";
            const disclosure = via.length
              ? `<details class="board-via">
                  <summary>${viaSummary}</summary>
                  <ol>${via.map((stop) => `<li>${escapeHtml(stop)}</li>`).join("")}</ol>
                </details>`
              : `<div class="board-direct">${viaSummary}</div>`;
            return `<div class="board-departure" data-departure-index="${departureIndex}">
              <div class="board-row">
                <time>${fmtShortTime(departure.time, timeZone)}</time>
                <b>${escapeHtml(departure.line || "Train")}</b>
                <span>${escapeHtml(stationName(dataset, destination))}</span>
              </div>
              ${disclosure}
            </div>`;
          })
          .join("")
      : `<div class="empty">${departures.length > 0
          ? "No departures match the selected train services."
          : covered
            ? "No observed departures in the next three simulated hours."
            : `No ${escapeHtml(datasets[dataset]!.name)} data is published for this date.`}</div>`;
    showDetails(`
      <div class="eyebrow">Station board · observed data</div>
      <h2>${escapeHtml(station.name)}</h2>
      <div class="sub">15 minutes back · 3 hours ahead at ${fmtShortTime(clock.simTime, timeZone)}</div>
      <div class="board"><h3>Departures</h3>${body}</div>
      ${stationSpectateAction()}
      ${multiCountry ? sourceNote(dataset) : ""}`);
    for (const row of detailsContent.querySelectorAll<HTMLElement>("[data-departure-index]")) {
      const departure = renderedDepartures[Number(row.dataset.departureIndex)];
      if (!departure) continue;
      const highlight = () => {
        highlightedStationRoutes = { dataset, routeIds: [...departure.routeIds] };
      };
      row.addEventListener("pointerenter", highlight);
      row.addEventListener("pointerleave", () => {
        highlightedStationRoutes = null;
      });
      row.addEventListener("focusin", highlight);
      row.addEventListener("focusout", (event) => {
        if (!row.contains(event.relatedTarget as Node | null)) highlightedStationRoutes = null;
      });
    }
  }

  const showSpeedCommands = () => {
    commandContext.textContent = "Playback · automatically running unless paused";
    commandResults.innerHTML = "";
    for (const speed of SPEEDS) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `command-item${clock.speed === speed && !clock.paused ? " active" : ""}`;
      button.innerHTML = `<span class="token">${speed}×</span><span><strong>Run at ${speed}×</strong><small>${speed === 600 ? "Default observer speed" : "Historical playback speed"}</small></span><kbd>Enter</kbd>`;
      button.onclick = () => {
        clock.setSpeed(speed);
        clock.play();
        speedValue.textContent = `${speed}×`;
        closeCommand();
      };
      commandResults.appendChild(button);
    }
    const pause = document.createElement("button");
    pause.type = "button";
    pause.className = `command-item${clock.paused ? " active" : ""}`;
    pause.innerHTML = `<span class="token">Ⅱ</span><span><strong>${clock.paused ? "Resume" : "Pause"}</strong><small>Keep the current historical instant</small></span><kbd>Space</kbd>`;
    pause.onclick = () => {
      togglePlayback();
      speedValue.textContent = clock.paused ? "Paused" : `${clock.speed}×`;
      closeCommand();
    };
    commandResults.appendChild(pause);
  };

  const showTrainSearchPrompt = () => {
    commandContext.textContent = "Find a train on the displayed day";
    commandResults.innerHTML = `<div class="empty">Search by train number, journey identity, or line — for example IC5, S1, or ICE.${multiCountry ? " Type a country name to fly there." : ""}</div>`;
  };

  let commandHome: "search" | "speed" = "search";
  const openCommand = (home: "search" | "speed" = "search") => {
    commandHome = home;
    command.classList.remove("hidden");
    commandInput.value = "";
    if (home === "speed") showSpeedCommands();
    else showTrainSearchPrompt();
    requestAnimationFrame(() => commandInput.focus());
  };
  const closeCommand = () => command.classList.add("hidden");
  byId<HTMLButtonElement>("command-button").onclick = () => openCommand("search");
  speedBadge.onclick = () => openCommand("speed");
  command.onclick = (event) => {
    if (event.target === command) closeCommand();
  };

  /** Seek without losing the wall-clock hour, landing on the nearest day the country covers. */
  const nearestCoveredTime = (dataset: number, time: number): number => {
    const days = Object.keys(manifests[dataset]!.days).sort();
    const today = utcDay(time);
    if (days.length === 0 || today in manifests[dataset]!.days) return time;
    // First covered day on or after today, or the last one; ISO dates sort as strings.
    const after = days.find((day) => day > today);
    const before = days.filter((day) => day < today).at(-1);
    const distance = (day: string) => Math.abs(Date.parse(day) - Date.parse(today));
    const target = after && (!before || distance(after) <= distance(before)) ? after : before!;
    return Date.parse(`${target}T00:00:00Z`) / 1000 + (((time % 86_400) + 86_400) % 86_400);
  };

  const seekTo = (time: number) => {
    clock.seek(time);
    win = { from: 0, to: -1, datasets: [], legs: [] };
  };

  function flyToDataset(dataset: number): void {
    const [west, south, east, north] = datasets[dataset]!.bbox;
    closeCommand();
    const time = nearestCoveredTime(dataset, clock.simTime);
    if (time !== clock.simTime) {
      stopSpectating();
      seekTo(time);
    }
    map.fitBounds([west, south, east, north], { padding: 48, duration: 900 });
    void ensureView(dataset);
  }

  const countryMatches = (query: string): number[] => {
    const needle = query.trim().toLocaleLowerCase();
    if (!multiCountry || needle.length < 2) return [];
    return datasets
      .filter((info) => info.name.toLocaleLowerCase().startsWith(needle) || info.country.toLocaleLowerCase() === needle)
      .map((info) => info.index);
  };

  const renderCountryItems = (indexes: number[]) => {
    for (const index of indexes) {
      const info = datasets[index]!;
      const manifest = manifests[index]!;
      const covered = utcDay(clock.simTime) in manifest.days;
      const button = document.createElement("button");
      button.type = "button";
      button.className = "command-item";
      button.innerHTML = `<span class="token">${escapeHtml(info.country)}</span><span><strong>Fly to ${escapeHtml(info.name)}</strong><small>${escapeHtml(info.provider)} · ${manifest.start} → ${manifest.end}${covered ? "" : " · jumps to the nearest covered day"}</small></span><kbd>Enter</kbd>`;
      button.onclick = () => flyToDataset(index);
      commandResults.appendChild(button);
    }
  };

  let searchGeneration = 0;
  let searchTimer = 0;
  commandInput.oninput = () => {
    window.clearTimeout(searchTimer);
    const query = commandInput.value.trim();
    if (!query) {
      if (commandHome === "speed") showSpeedCommands();
      else showTrainSearchPrompt();
      return;
    }
    const timeZone = focusDataset().timezone;
    const requestedTime = zonedEpoch(query, timeZone);
    if (requestedTime !== null) {
      commandContext.textContent = `Historical navigation · ${timeZone} time`;
      commandResults.innerHTML = "";
      const button = document.createElement("button");
      button.type = "button";
      button.className = "command-item";
      const inRange = requestedTime >= tMin && requestedTime <= tMax;
      button.disabled = !inRange;
      button.innerHTML = `<span class="token">GO</span><span><strong>${escapeHtml(fmtClock(requestedTime, timeZone))}</strong><small>${inRange ? "Jump to this instant, then search trains on that day" : "Outside the published archive"}</small></span><kbd>Enter</kbd>`;
      button.onclick = () => {
        seekTo(requestedTime);
        selectedJourney = null;
        selectedJourneyTracks = [];
        closeCommand();
        void refill(requestedTime);
      };
      commandResults.appendChild(button);
      return;
    }
    const countries = countryMatches(query);
    commandContext.textContent = `Searching trains on ${utcDay(clock.simTime)}…`;
    commandResults.innerHTML = "";
    renderCountryItems(countries);
    commandResults.insertAdjacentHTML("beforeend", `<div class="empty">Querying the journey sidecar…</div>`);
    const generation = ++searchGeneration;
    searchTimer = window.setTimeout(async () => {
      try {
        const response = await ask(worker, {
          kind: "search",
          query,
          simTime: clock.simTime,
          datasets: searchDatasets(),
        });
        if (generation !== searchGeneration || response.kind !== "search-results") return;
        renderSearchResults(response.day, response.results, countries);
      } catch (error) {
        commandResults.innerHTML = `<div class="empty">Search failed: ${escapeHtml(String(error))}</div>`;
      }
    }, 180);
  };

  /** Search the countries in view first; with none in view, every country with static data. */
  const searchDatasets = (): number[] => {
    const loaded = new Set(loadedViews().map((view) => view.info.index));
    const inView = visibleDatasets().filter((index) => loaded.has(index));
    return inView.length > 0 ? inView : [...loaded];
  };

  function renderSearchResults(day: string, results: JourneySearchResult[], countries: number[]): void {
    commandContext.textContent = `Trains on ${day} · search uses exact published journey identities and lines`;
    commandResults.innerHTML = "";
    renderCountryItems(countries);
    if (results.length === 0) {
      commandResults.insertAdjacentHTML(
        "beforeend",
        `<div class="empty">No matching train on this day. Search a line such as IC5, S1, ICE, or a train number contained in its trip ID.</div>`,
      );
      return;
    }
    for (const result of results) {
      const info = datasets[result.dataset]!;
      const route = routeDescription(result.dataset, result.firstRouteId);
      const number = trainNumber(result.tripId, info.id);
      const button = document.createElement("button");
      button.type = "button";
      button.className = "command-item";
      button.innerHTML = `
        <span class="token">${escapeHtml(result.line || number)}</span>
        <span><strong>${escapeHtml(route.from)} → ${escapeHtml(route.to)}${multiCountry ? `<span class="country-tag">${escapeHtml(info.country)}</span>` : ""}</strong>
        <small>Train ${escapeHtml(number)} · ${escapeHtml(result.tripId)}</small></span>
        <time>${fmtShortTime(result.start, info.timezone)}</time>`;
      button.onclick = () => void watchJourney(result);
      commandResults.appendChild(button);
    }
  }

  async function watchJourney(
    result: JourneySearchResult,
    seekToStart = true,
    generation = ++spectateGeneration,
  ): Promise<void> {
    if (generation !== spectateGeneration) return;
    stationBoardGeneration++;
    visibleStationBoard = null;
    highlightedStationRoutes = null;
    selectedJourney = result;
    selectedJourneyTracks = [];
    clock.setSpeed(1);
    clock.play();
    speedValue.textContent = "1×";
    const info = datasets[result.dataset]!;
    const number = trainNumber(result.tripId, info.id);
    const watchTime = seekToStart ? Math.max(tMin, result.start) : clock.simTime;
    if (seekToStart) seekTo(watchTime);
    const route = routeDescription(result.dataset, result.firstRouteId);
    showDetails(`
      <div class="eyebrow">Spectating train</div>
      <h2>${escapeHtml(result.line || `Train ${number}`)}</h2>
      <div class="sub">${escapeHtml(route.from)} → ${escapeHtml(route.to)} · ${seekToStart ? "departs" : "started"} ${fmtShortTime(result.start, info.timezone)}</div>
      <div class="board"><h3>Loading train…</h3><div class="empty">${escapeHtml(result.tripId)}</div></div>
      <div class="watch-actions"><button class="primary" data-stop-spectating type="button">Stop spectating</button></div>`);
    closeCommand();
    await refill(watchTime);

    const stillSelected = () =>
      generation === spectateGeneration &&
      selectedJourney?.dataset === result.dataset &&
      selectedJourney.journeyId === result.journeyId;
    // A newer selection may have replaced this one while its remote window was loading.
    if (!stillSelected()) return;
    try {
      const routeResponse = await ask(worker, {
        kind: "journey-route",
        dataset: result.dataset,
        journeyId: result.journeyId,
        simTime: result.start,
      });
      if (stillSelected() && routeResponse.kind === "journey-route-result") {
        selectedJourneyTracks = routeResponse.routeIds
          .map((routeId) => trackPathById.get(trackKey(result.dataset, routeId)))
          .filter((route): route is TrackPath => route !== undefined);
      }
    } catch (error) {
      console.warn("full journey route unavailable", error);
    }
    if (!stillSelected()) return;
    const ref = { dataset: result.dataset, journeyId: result.journeyId };
    const selected = positioned(
      win.legs,
      clock.simTime,
      routesFor,
      Math.max(30, Math.min(8_000, metersPerPixel(map.getCenter().lat, map.getZoom()) * 6)),
    ).items.find((item) => isJourney(item.leg, ref));
    if (selected) {
      map.jumpTo({
        center: selected.pos,
        zoom: Math.max(map.getZoom(), 13),
      });
    }
    showDetails(`
      <div class="eyebrow">Spectating train · 1× playback</div>
      <h2>${escapeHtml(result.line || `Train ${number}`)}</h2>
      <div class="sub">${escapeHtml(route.from)} → ${escapeHtml(route.to)} · departed ${fmtShortTime(result.start, info.timezone)}</div>
      <div class="board"><h3>Journey identity</h3><div class="empty">${escapeHtml(result.tripId)}</div></div>
      <div class="watch-actions"><button class="primary" data-stop-spectating type="button">Stop spectating</button></div>
      ${multiCountry ? sourceNote(result.dataset) : ""}`);
  }

  async function spectatePositionedTrain(item: PositionedLeg): Promise<void> {
    const generation = ++spectateGeneration;
    stationBoardGeneration++;
    visibleStationBoard = null;
    highlightedStationRoutes = null;
    selectedJourney = null;
    selectedJourneyTracks = [];
    clock.setSpeed(1);
    clock.play();
    speedValue.textContent = "1×";
    const { dataset } = item.leg;
    const route = routeDescription(dataset, item.leg.route_id);
    showDetails(`
      <div class="eyebrow">Selecting train · 1× playback</div>
      <h2>${escapeHtml(typeName(dataset, item.leg.type))}</h2>
      <div class="sub">${escapeHtml(route.from)} → ${escapeHtml(route.to)}</div>
      <div class="board"><h3>Loading journey identity…</h3></div>`);
    try {
      const response = await ask(worker, {
        kind: "journey",
        dataset,
        journeyId: item.leg.journey_id,
        // Journey IDs are only unique inside the file containing this leg.
        simTime: item.leg.t_dep,
      });
      if (generation !== spectateGeneration) return;
      if (response.kind !== "journey-result" || !response.result) {
        showDetails(`
          <div class="eyebrow">Train unavailable</div>
          <h2>${escapeHtml(typeName(dataset, item.leg.type))}</h2>
          <div class="empty">The journey identity could not be loaded for this train.</div>`);
        return;
      }
      await watchJourney(response.result, false, generation);
    } catch (error) {
      if (generation !== spectateGeneration) return;
      showDetails(`
        <div class="eyebrow">Train unavailable</div>
        <h2>${escapeHtml(typeName(dataset, item.leg.type))}</h2>
        <div class="empty">Could not load this train: ${escapeHtml(String(error))}</div>`);
    }
  }

  document.addEventListener("keydown", (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLocaleLowerCase() === "k") {
      event.preventDefault();
      if (command.classList.contains("hidden")) openCommand("search");
      else closeCommand();
    } else if (event.key === "Escape" && !command.classList.contains("hidden")) {
      closeCommand();
    } else if (event.code === "Space" && command.classList.contains("hidden")) {
      event.preventDefault();
      togglePlayback();
      speedValue.textContent = clock.paused ? "Paused" : `${clock.speed}×`;
    }
  });

  const routesFor = (dataset: number): Routes | undefined => views[dataset]?.routes;

  /**
   * The countries whose facts the window should hold: those in view with static geometry
   * loaded, plus the spectated train's. A country off screen costs no range requests.
   */
  const wantedDatasets = (): number[] => {
    const wanted = new Set(visibleDatasets().filter((index) => views[index] !== undefined));
    if (selectedJourney) wanted.add(selectedJourney.dataset);
    return [...wanted].sort((a, b) => a - b);
  };

  let win: { from: number; to: number; datasets: number[]; legs: Leg[] } = {
    from: 0,
    to: -1,
    datasets: [],
    legs: [],
  };
  let refillInFlight: Promise<void> | null = null;
  const lookahead = () => Math.max(BUFFER_SECONDS, BUFFER_SECONDS * (clock.speed / 60));
  // Keep two wall-clock seconds of data in hand. A fixed 60 simulated-second margin was only
  // 100 ms at 600×, so a normal range request could exhaust the window and flash the map.
  const refetchMargin = () => Math.max(REFETCH_MARGIN_S, clock.speed * 2);
  const windowCovers = (time: number, wanted: number[]): boolean =>
    time >= win.from && time <= win.to - refetchMargin() && wanted.every((index) => win.datasets.includes(index));
  async function refill(time: number): Promise<void> {
    // If another window is being fetched, wait for it and then decide whether it covered this
    // seek. Spectating must not silently lose its load because ordinary playback was fetching.
    while (refillInFlight) await refillInFlight;
    const wanted = wantedDatasets();
    if (windowCovers(time, wanted)) return;

    const request = (async () => {
      try {
        const response = await ask(worker, {
          kind: "window",
          simTime: time,
          lookahead: lookahead(),
          datasets: wanted,
        });
        if (response.kind === "window") win = response;
      } catch (error) {
        console.error("window fetch failed", error);
      }
    })();
    refillInFlight = request;
    try {
      await request;
    } finally {
      if (refillInFlight === request) refillInFlight = null;
    }
  }

  /** A country in view whose facts do not reach this instant is missing coverage, not idle. */
  const coverageNotice = (time: number): string => {
    const day = utcDay(time);
    for (const index of visibleDatasets()) {
      const manifest = manifests[index]!;
      const info = datasets[index]!;
      if (manifest.source_cancelled_days?.includes(day)) {
        return `Most ${info.name} trains were cancelled on ${day} · recorded by the source`;
      }
      if (day in manifest.days) continue;
      return manifest.start && manifest.end && day >= manifest.start && day <= manifest.end
        ? `No ${info.name} data for ${day} · source gap`
        : `No ${info.name} data for ${day} · covered ${manifest.start} → ${manifest.end}`;
    }
    return "";
  };

  setProgress(82, "Fetching the first train window…");
  refreshStaticLayers();
  await refill(clock.simTime);
  setProgress(100, "Ready");
  topbar.classList.remove("hidden");
  loading.classList.add("done");

  const scheduleFrame = (callback: FrameRequestCallback) => {
    if (e2eMode) window.setTimeout(() => requestAnimationFrame(callback), 5_000);
    else requestAnimationFrame(callback);
  };

  function frame(): void {
    const time = clock.tick();
    if (time > tMax) clock.seek(tMin);
    for (const index of visibleDatasets()) {
      if (!viewLoads.has(index)) void ensureView(index);
    }
    refreshStaticLayers();
    if (!windowCovers(time, wantedDatasets())) void refill(time);

    const selected = selectedRef();
    const relevantLegs = win.legs.filter(
      (leg) => typeEnabled(leg.dataset, leg.type) || isJourney(leg, selected),
    );
    // Average the route tangent across roughly six screen pixels. At national zoom this removes
    // noisy vertex-to-vertex heading changes; close up it converges to the precise local track.
    const bearingWindowM = Math.max(
      30,
      Math.min(8_000, metersPerPixel(map.getCenter().lat, map.getZoom()) * 6),
    );
    const { items, dropped } = positioned(relevantLegs, time, routesFor, bearingWindowM);
    e2ePositionedTrains = items;
    const selectedItem = selected === null ? undefined : items.find((item) => isJourney(item.leg, selected));
    const selectedTrack = selectedItem
      ? trackPathById.get(trackKey(selectedItem.leg.dataset, selectedItem.leg.route_id))
      : undefined;
    const stationBoardTracks = highlightedStationRoutes
      ? highlightedStationRoutes.routeIds
          .map((routeId) => trackPathById.get(trackKey(highlightedStationRoutes!.dataset, routeId)))
          .filter((route): route is TrackPath => route !== undefined)
      : [];
    const highlightedTracks = stationBoardTracks.length > 0
      ? stationBoardTracks
      : selectedJourneyTracks.length > 0
        ? selectedJourneyTracks
        : selectedTrack
          ? [selectedTrack]
          : [];
    const selectedTrackLayer = new PathLayer<TrackPath>({
      id: "selected-train-track",
      data: highlightedTracks,
      getPath: (route) => route.path,
      getColor: [255, 0, 0, 235],
      getWidth: 4,
      widthUnits: "pixels",
      widthMinPixels: 3,
      capRounded: true,
      jointRounded: true,
      pickable: false,
    });
    const trainIcons = trainsLayer(
      items,
      time,
      typeColor,
      colorMode,
      selected,
      map.getBearing(),
      map.getZoom(),
      (item) => void spectatePositionedTrain(item),
    );
    overlay.setProps({
      // The full national track layer contains 512k vertices. Browser tests validate its
      // selected route IDs but omit that visual layer so software WebGL can keep up in CI.
      layers: e2eMode
        ? [stationLayer, trainIcons]
        : [trackLayer, countryBorderLayer, selectedTrackLayer, stationLayer, trainIcons],
    });

    if (selectedJourney) {
      // Keep the camera locked to the interpolated position. Repeated easeTo calls restart an
      // animation and visibly hitch at 8×; a direct per-frame center update stays continuous.
      if (selectedItem) map.setCenter(selectedItem.pos);
      if (time > selectedJourney.end + 60) {
        selectedJourney = null;
        selectedJourneyTracks = [];
      }
    }

    const focus = focusDataset();
    const zone = multiCountry ? ` · ${focus.timezone.split("/").at(-1)!.replaceAll("_", " ")}` : "";
    hudDateElement.textContent = `${fmtHudDate(time, focus.timezone)}${zone}`;
    timeElement.textContent = fmtHudTime(time, focus.timezone);
    timeElement.dateTime = new Date(time * 1000).toISOString();
    playbackIcon.textContent = clock.paused ? "▶" : "Ⅱ";
    playbackLabel.textContent = clock.paused ? "Resume playback" : "Pause playback";
    speedValue.textContent = clock.paused ? "Paused" : `${clock.speed}×`;
    countElement.textContent = `${items.length.toLocaleString()} trains${dropped ? ` · ${dropped} unplaced` : ""}`;
    const notice = coverageNotice(time);
    coverageElement.textContent = notice;
    coverageElement.classList.toggle("hidden", notice === "");
    scheduleFrame(frame);
  }
  requestAnimationFrame(frame);
}

main().catch((error: unknown) => {
  console.error(error);
  const loadingLabel = document.getElementById("loading-label");
  if (loadingLabel) loadingLabel.textContent = "Could not start";
  const errorElement = document.getElementById("error");
  if (errorElement) {
    errorElement.textContent = error instanceof Error ? error.message : String(error);
    errorElement.classList.remove("hidden");
  }
});
