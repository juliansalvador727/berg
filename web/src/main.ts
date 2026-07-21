/** Browser-only historical train observer. */

import { MapboxOverlay } from "@deck.gl/mapbox";
import { PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import maplibregl from "maplibre-gl";
import "@fontsource-variable/noto-sans";
import "maplibre-gl/dist/maplibre-gl.css";
import "./style.css";

import { Clock, SPEEDS } from "./clock";
import {
  BUFFER_SECONDS,
  INITIAL_VIEW,
  MAP_STYLE_URL,
  ROUTE_PAIRS_URL,
  ROUTES_URL,
  STATIONS_URL,
  TERRAIN_TILE_URL,
  TRAIN_TYPES_URL,
} from "./config";
import { fetchRoutes, type RoutePath, type Routes } from "./routes";
import {
  type ColorMode,
  delayColor,
  positioned,
  PUNCTUAL_S,
  trainsLayer,
  type PositionedLeg,
  typeColors,
} from "./render/trains";
import { FLAG_SCHEDULED_FALLBACK, type Leg, type Manifest } from "./types";
import type {
  JourneySearchResult,
  StationBoardLeg,
  WorkerRequestPayload,
  WorkerResponse,
} from "./worker/legs.worker";

interface Station {
  id: number;
  name: string;
  lon: number;
  lat: number;
}

interface ServiceGroup {
  id: string;
  label: string;
  codes: ReadonlySet<string>;
}

const SERVICE_GROUPS: ServiceGroup[] = [
  { id: "s", label: "S-Bahn", codes: new Set(["S", "SN"]) },
  { id: "regional", label: "Regional", codes: new Set(["R", "RB", "RE", "IRE", "TER", "PE"]) },
  { id: "intercity", label: "IC / IR", codes: new Set(["IC", "IR"]) },
  { id: "fast", label: "ICE / fast", codes: new Set(["ICE", "TGV", "EC", "RJ", "RJX"]) },
  { id: "night", label: "Night", codes: new Set(["NJ", "EN", "NZ"]) },
];

const REFETCH_MARGIN_S = 60;
const DEFAULT_DAY = "2018-01-01";
let nextRequestId = 1;

/** Approximate ground resolution at the map center for zoom-adaptive arrow smoothing. */
const metersPerPixel = (latitude: number, zoom: number): number =>
  (156_543.03392 * Math.cos((latitude * Math.PI) / 180)) / 2 ** zoom;

const byId = <T extends HTMLElement>(id: string): T => {
  const element = document.getElementById(id);
  if (!element) throw new Error(`missing #${id}`);
  return element as T;
};

const escapeHtml = (value: string): string =>
  value.replace(/[&<>'"]/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[char]!);

const fmtClock = (epoch: number): string =>
  new Date(epoch * 1000).toLocaleString("de-CH", {
    timeZone: "Europe/Zurich",
    weekday: "short",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

const fmtShortTime = (epoch: number): string =>
  new Date(epoch * 1000).toLocaleTimeString("de-CH", {
    timeZone: "Europe/Zurich",
    hour: "2-digit",
    minute: "2-digit",
  });

const fmtHudDate = (epoch: number): string =>
  new Date(epoch * 1000).toLocaleDateString("de-CH", {
    timeZone: "Europe/Zurich",
    weekday: "short",
    day: "2-digit",
    month: "long",
    year: "numeric",
  });

const fmtHudTime = (epoch: number): string =>
  new Date(epoch * 1000).toLocaleTimeString("de-CH", {
    timeZone: "Europe/Zurich",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

const trainNumber = (tripId: string): string => {
  const parts = tripId.split(":");
  if (tripId.includes(":sjyid:")) return parts[parts.length - 1]?.split("-")[0] ?? tripId;
  return parts.length >= 3 ? parts[parts.length - 2]! : tripId;
};

/** Convert a Europe/Zurich wall-clock value without depending on the viewer's own timezone. */
function zurichEpoch(value: string): number | null {
  const match = /^(\d{4}-\d{2}-\d{2})(?:[ T](\d{1,2}):(\d{2}))?$/.exec(value.trim());
  if (!match) return null;
  const hour = Number(match[2] ?? 8);
  const minute = Number(match[3] ?? 0);
  if (hour > 23 || minute > 59) return null;
  const desiredAsUtc = Date.parse(`${match[1]}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00Z`);
  if (!Number.isFinite(desiredAsUtc)) return null;
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Zurich",
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

async function main(): Promise<void> {
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
  const manifest: Manifest = ready.manifest;
  if (!manifest.start || !manifest.end) throw new Error("manifest advertises no days");

  setProgress(24, "Loading routes, stations, and train classes…");
  const [routes, allStations, typeMap, routePairs] = await Promise.all([
    fetchRoutes(ROUTES_URL) as Promise<Routes>,
    fetch(STATIONS_URL).then((response) => response.json() as Promise<Station[]>),
    fetch(TRAIN_TYPES_URL).then((response) => response.json() as Promise<Record<string, string>>),
    fetch(ROUTE_PAIRS_URL).then(
      (response) => response.json() as Promise<Record<string, [number, number]>>,
    ),
  ]);

  setProgress(48, "Preparing the observed rail network…");
  const served = new Set<number>();
  const stationRouteIds = new Map<number, number[]>();
  for (const [routeIdText, [from, to]] of Object.entries(routePairs)) {
    const routeId = Number(routeIdText);
    served.add(from);
    served.add(to);
    stationRouteIds.set(from, [...(stationRouteIds.get(from) ?? []), routeId]);
    stationRouteIds.set(to, [...(stationRouteIds.get(to) ?? []), routeId]);
  }
  const stations = allStations.filter((station) => served.has(station.id));
  const stationById = new Map(stations.map((station) => [station.id, station]));
  const trackPaths = routes.paths();
  const trackPathById = new Map(trackPaths.map((route) => [route.routeId, route]));

  const maxType = Math.max(...Object.keys(typeMap).map(Number));
  const types = Array.from({ length: maxType + 1 }, (_, index) => typeMap[String(index)] ?? "?");
  const colors = typeColors(types);
  const knownGroupedTypes = new Set(SERVICE_GROUPS.flatMap((group) => [...group.codes]));
  const allGroups = [
    ...SERVICE_GROUPS,
    {
      id: "other",
      label: "Other",
      codes: new Set(types.filter((type) => !knownGroupedTypes.has(type))),
    },
  ];
  const enabledGroups = new Set(allGroups.map((group) => group.id));

  setProgress(64, "Loading the dark map and mountain relief…");
  const map = new maplibregl.Map({
    container: "map",
    style: MAP_STYLE_URL,
    center: [INITIAL_VIEW.longitude, INITIAL_VIEW.latitude],
    zoom: INITIAL_VIEW.zoom,
    pitch: 24,
    bearing: 0,
    maxPitch: 70,
    attributionControl: false,
  });
  await map.once("load");

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

  const overlay = new MapboxOverlay({
    interleaved: false,
    layers: [],
    getCursor: ({ isDragging, isHovering }) =>
      isDragging ? "grabbing" : isHovering ? "pointer" : "grab",
  });
  map.addControl(overlay);

  const days = Object.keys(manifest.days).sort();
  const substantialDays = days.filter((day) => manifest.days[day]!.legs >= 10_000);
  const initialDay = DEFAULT_DAY in manifest.days
    ? DEFAULT_DAY
    : (substantialDays[substantialDays.length - 1] ?? days[days.length - 1]!);
  const tMin = Date.parse(`${days[0]}T00:00:00Z`) / 1000;
  const tMax = Date.parse(`${days[days.length - 1]}T23:59:59Z`) / 1000;
  const clock = new Clock(Date.parse(`${initialDay}T06:00:00Z`) / 1000);
  clock.setSpeed(600);
  clock.play();

  const topbar = byId<HTMLElement>("topbar");
  const filters = byId<HTMLElement>("filters");
  const hudDateElement = byId<HTMLSpanElement>("hud-date");
  const timeElement = byId<HTMLTimeElement>("time");
  const countElement = byId<HTMLSpanElement>("count");
  const speedBadge = byId<HTMLButtonElement>("speed-badge");
  const playbackButton = byId<HTMLButtonElement>("playback-button");
  const playbackIcon = byId<HTMLSpanElement>("playback-icon");
  const playbackLabel = byId<HTMLElement>("playback-label");
  const filterButton = byId<HTMLButtonElement>("filter-button");
  const filterSummary = byId<HTMLElement>("filter-summary");
  const archiveMeta = byId<HTMLDivElement>("archive-meta");
  const details = byId<HTMLElement>("details");
  const detailsContent = byId<HTMLDivElement>("details-content");
  const command = byId<HTMLDivElement>("command");
  const commandInput = byId<HTMLInputElement>("command-input");
  const commandContext = byId<HTMLDivElement>("command-context");
  const commandResults = byId<HTMLDivElement>("command-results");
  const filterChips = byId<HTMLDivElement>("filter-chips");
  const legendElement = byId<HTMLDivElement>("legend");
  let colorMode: ColorMode = "type";

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

  archiveMeta.textContent = `${days.length.toLocaleString()} days · ${routes.length.toLocaleString()} routes · ${stations.length.toLocaleString()} stations`;

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
    };
    filterChips.appendChild(button);
  }

  const typeEnabled = (typeId: number): boolean => {
    const code = types[typeId] ?? "?";
    return allGroups.some((group) => enabledGroups.has(group.id) && group.codes.has(code));
  };

  const swatch = (color: [number, number, number], label: string): string =>
    `<span class="key"><i style="background:rgb(${color.join(",")})"></i>${label}</span>`;
  const modeButtons: Record<ColorMode, HTMLElement> = {
    type: byId("mode-type"),
    delay: byId("mode-delay"),
  };
  const renderLegend = () => {
    const sample = (delay: number, flags = 0): Leg => ({
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

  const trackLayer = new PathLayer<RoutePath>({
    id: "observed-rail-network",
    data: trackPaths,
    getPath: (route) => route.path,
    getColor: (route) => (route.fallback ? [92, 102, 116, 30] : [105, 124, 143, 85]),
    getWidth: 1,
    widthUnits: "pixels",
    widthMinPixels: 0.65,
    pickable: false,
  });

  let selectedJourney: JourneySearchResult | null = null;
  let selectedJourneyTracks: RoutePath[] = [];
  let spectateGeneration = 0;

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
  byId<HTMLButtonElement>("details-close").onclick = () => stopSpectating();

  const stationLayer = new ScatterplotLayer<Station>({
    id: "stations",
    data: stations,
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

  function stationName(id: number): string {
    return stationById.get(id)?.name ?? `Station ${id}`;
  }

  function routeDescription(routeId: number): { from: string; to: string } {
    const pair = routePairs[String(routeId)];
    return pair
      ? { from: stationName(pair[0]), to: stationName(pair[1]) }
      : { from: "Unknown origin", to: "Unknown destination" };
  }

  async function openStation(station: Station): Promise<void> {
    showDetails(`
      <div class="eyebrow">Station</div>
      <h2>${escapeHtml(station.name)}</h2>
      <div class="sub">Loading observed arrivals and departures…</div>
      ${stationSpectateAction()}`);
    map.easeTo({ center: [station.lon, station.lat], zoom: Math.max(map.getZoom(), 11), duration: 650 });
    try {
      const response = await ask(worker, {
        kind: "station-board",
        routeIds: stationRouteIds.get(station.id) ?? [],
        simTime: clock.simTime,
        horizon: 3 * 3600,
      });
      if (response.kind !== "station-board") return;
      renderStationBoard(station, response.legs);
    } catch (error) {
      showDetails(`
        <div class="eyebrow">Station</div><h2>${escapeHtml(station.name)}</h2>
        <div class="empty">Could not load the board: ${escapeHtml(String(error))}</div>
        ${stationSpectateAction()}`);
    }
  }

  function renderStationBoard(station: Station, legs: StationBoardLeg[]): void {
    const rows: { time: number; kind: "arr" | "dep"; other: string; line: string }[] = [];
    for (const leg of legs) {
      const pair = routePairs[String(leg.route_id)];
      if (!pair) continue;
      if (pair[0] === station.id && leg.t_dep >= clock.simTime - 15 * 60) {
        rows.push({ time: leg.t_dep, kind: "dep", other: stationName(pair[1]), line: leg.line });
      }
      const arrival = leg.t_dep + leg.dur;
      if (pair[1] === station.id && arrival >= clock.simTime - 15 * 60) {
        rows.push({ time: arrival, kind: "arr", other: stationName(pair[0]), line: leg.line });
      }
    }
    rows.sort((a, b) => a.time - b.time);
    const visible = rows.slice(0, 36);
    const body = visible.length
      ? visible
          .map(
            (row) => `<div class="board-row">
              <time>${fmtShortTime(row.time)}</time>
              <b>${escapeHtml(row.line || "Train")}</b>
              <span>${row.kind === "dep" ? "to" : "from"} ${escapeHtml(row.other)}</span>
              <em>${row.kind}</em>
            </div>`,
          )
          .join("")
      : `<div class="empty">No observed movements in the next three simulated hours.</div>`;
    showDetails(`
      <div class="eyebrow">Station board · observed data</div>
      <h2>${escapeHtml(station.name)}</h2>
      <div class="sub">15 minutes back · 3 hours ahead at ${fmtShortTime(clock.simTime)}</div>
      <div class="board"><h3>Arrivals & departures</h3>${body}</div>
      ${stationSpectateAction()}`);
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
        speedBadge.textContent = `${speed}×`;
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
      speedBadge.textContent = clock.paused ? "paused" : `${clock.speed}×`;
      closeCommand();
    };
    commandResults.appendChild(pause);
  };

  const showTrainSearchPrompt = () => {
    commandContext.textContent = "Find a train on the displayed day";
    commandResults.innerHTML = `<div class="empty">Search by train number, journey identity, or line — for example IC5, S1, or ICE.</div>`;
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
    const requestedTime = zurichEpoch(query);
    if (requestedTime !== null) {
      commandContext.textContent = "Historical navigation · Europe/Zurich time";
      commandResults.innerHTML = "";
      const button = document.createElement("button");
      button.type = "button";
      button.className = "command-item";
      const inRange = requestedTime >= tMin && requestedTime <= tMax;
      button.disabled = !inRange;
      button.innerHTML = `<span class="token">GO</span><span><strong>${escapeHtml(fmtClock(requestedTime))}</strong><small>${inRange ? "Jump to this instant, then search trains on that day" : "Outside the published archive"}</small></span><kbd>Enter</kbd>`;
      button.onclick = () => {
        clock.seek(requestedTime);
        win = { from: 0, to: -1, legs: [] };
        selectedJourney = null;
        selectedJourneyTracks = [];
        closeCommand();
        void refill(requestedTime);
      };
      commandResults.appendChild(button);
      return;
    }
    commandContext.textContent = `Searching trains on ${new Date(clock.simTime * 1000).toISOString().slice(0, 10)}…`;
    commandResults.innerHTML = `<div class="empty">Querying the journey sidecar…</div>`;
    const generation = ++searchGeneration;
    searchTimer = window.setTimeout(async () => {
      try {
        const response = await ask(worker, { kind: "search", query, simTime: clock.simTime });
        if (generation !== searchGeneration || response.kind !== "search-results") return;
        renderSearchResults(response.day, response.results);
      } catch (error) {
        commandResults.innerHTML = `<div class="empty">Search failed: ${escapeHtml(String(error))}</div>`;
      }
    }, 180);
  };

  function renderSearchResults(day: string, results: JourneySearchResult[]): void {
    commandContext.textContent = `Trains on ${day} · search uses exact published journey identities and lines`;
    commandResults.innerHTML = "";
    if (results.length === 0) {
      commandResults.innerHTML = `<div class="empty">No matching train on this day. Search a line such as IC5, S1, ICE, or a train number contained in its trip ID.</div>`;
      return;
    }
    for (const result of results) {
      const route = routeDescription(result.firstRouteId);
      const button = document.createElement("button");
      button.type = "button";
      button.className = "command-item";
      button.innerHTML = `
        <span class="token">${escapeHtml(result.line || trainNumber(result.tripId))}</span>
        <span><strong>${escapeHtml(route.from)} → ${escapeHtml(route.to)}</strong>
        <small>Train ${escapeHtml(trainNumber(result.tripId))} · ${escapeHtml(result.tripId)}</small></span>
        <time>${fmtShortTime(result.start)}</time>`;
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
    selectedJourney = result;
    selectedJourneyTracks = [];
    clock.setSpeed(1);
    clock.play();
    speedBadge.textContent = "1×";
    const watchTime = seekToStart ? Math.max(tMin, result.start) : clock.simTime;
    if (seekToStart) {
      clock.seek(watchTime);
      win = { from: 0, to: -1, legs: [] };
    }
    const route = routeDescription(result.firstRouteId);
    showDetails(`
      <div class="eyebrow">Spectating train</div>
      <h2>${escapeHtml(result.line || `Train ${trainNumber(result.tripId)}`)}</h2>
      <div class="sub">${escapeHtml(route.from)} → ${escapeHtml(route.to)} · ${seekToStart ? "departs" : "started"} ${fmtShortTime(result.start)}</div>
      <div class="board"><h3>Loading train…</h3><div class="empty">${escapeHtml(result.tripId)}</div></div>
      <div class="watch-actions"><button class="primary" data-stop-spectating type="button">Stop spectating</button></div>`);
    closeCommand();
    await refill(watchTime);

    // A newer selection may have replaced this one while its remote window was loading.
    if (generation !== spectateGeneration || selectedJourney?.journeyId !== result.journeyId) return;
    try {
      const routeResponse = await ask(worker, {
        kind: "journey-route",
        journeyId: result.journeyId,
        simTime: result.start,
      });
      if (
        generation === spectateGeneration &&
        selectedJourney?.journeyId === result.journeyId &&
        routeResponse.kind === "journey-route-result"
      ) {
        selectedJourneyTracks = routeResponse.routeIds
          .map((routeId) => trackPathById.get(routeId))
          .filter((route): route is RoutePath => route !== undefined);
      }
    } catch (error) {
      console.warn("full journey route unavailable", error);
    }
    if (generation !== spectateGeneration || selectedJourney?.journeyId !== result.journeyId) return;
    const selected = positioned(
      win.legs,
      clock.simTime,
      routes,
      Math.max(30, Math.min(8_000, metersPerPixel(map.getCenter().lat, map.getZoom()) * 6)),
    ).items.find(
      (item) => item.leg.journey_id === result.journeyId,
    );
    if (selected) {
      map.jumpTo({
        center: selected.pos,
        zoom: Math.max(map.getZoom(), 13),
      });
    }
    showDetails(`
      <div class="eyebrow">Spectating train · 1× playback</div>
      <h2>${escapeHtml(result.line || `Train ${trainNumber(result.tripId)}`)}</h2>
      <div class="sub">${escapeHtml(route.from)} → ${escapeHtml(route.to)} · departed ${fmtShortTime(result.start)}</div>
      <div class="board"><h3>Journey identity</h3><div class="empty">${escapeHtml(result.tripId)}</div></div>
      <div class="watch-actions"><button class="primary" data-stop-spectating type="button">Stop spectating</button></div>`);
  }

  async function spectatePositionedTrain(item: PositionedLeg): Promise<void> {
    const generation = ++spectateGeneration;
    selectedJourney = null;
    selectedJourneyTracks = [];
    clock.setSpeed(1);
    clock.play();
    speedBadge.textContent = "1×";
    const route = routeDescription(item.leg.route_id);
    showDetails(`
      <div class="eyebrow">Selecting train · 1× playback</div>
      <h2>${escapeHtml(types[item.leg.type] ?? "Train")}</h2>
      <div class="sub">${escapeHtml(route.from)} → ${escapeHtml(route.to)}</div>
      <div class="board"><h3>Loading journey identity…</h3></div>`);
    try {
      const response = await ask(worker, {
        kind: "journey",
        journeyId: item.leg.journey_id,
        // Journey IDs are only unique inside the file containing this leg.
        simTime: item.leg.t_dep,
      });
      if (generation !== spectateGeneration) return;
      if (response.kind !== "journey-result" || !response.result) {
        showDetails(`
          <div class="eyebrow">Train unavailable</div>
          <h2>${escapeHtml(types[item.leg.type] ?? "Train")}</h2>
          <div class="empty">The journey identity could not be loaded for this train.</div>`);
        return;
      }
      await watchJourney(response.result, false, generation);
    } catch (error) {
      if (generation !== spectateGeneration) return;
      showDetails(`
        <div class="eyebrow">Train unavailable</div>
        <h2>${escapeHtml(types[item.leg.type] ?? "Train")}</h2>
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
      speedBadge.textContent = clock.paused ? "paused" : `${clock.speed}×`;
    }
  });

  let win: { from: number; to: number; legs: Leg[] } = { from: 0, to: -1, legs: [] };
  let refillInFlight: Promise<void> | null = null;
  const lookahead = () => Math.max(BUFFER_SECONDS, BUFFER_SECONDS * (clock.speed / 60));
  // Keep two wall-clock seconds of data in hand. A fixed 60 simulated-second margin was only
  // 100 ms at 600×, so a normal range request could exhaust the window and flash the map.
  const refetchMargin = () => Math.max(REFETCH_MARGIN_S, clock.speed * 2);
  async function refill(time: number): Promise<void> {
    // If another window is being fetched, wait for it and then decide whether it covered this
    // seek. Spectating must not silently lose its load because ordinary playback was fetching.
    while (refillInFlight) await refillInFlight;
    if (time >= win.from && time <= win.to - refetchMargin()) return;

    const request = (async () => {
      try {
        const response = await ask(worker, { kind: "window", simTime: time, lookahead: lookahead() });
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

  setProgress(82, "Fetching the first train window…");
  await refill(clock.simTime);
  setProgress(100, "Ready");
  topbar.classList.remove("hidden");
  loading.classList.add("done");

  function frame(): void {
    const time = clock.tick();
    if (time > tMax) clock.seek(tMin);
    if (time < win.from || time > win.to - refetchMargin()) void refill(time);

    const selectedJourneyId = selectedJourney?.journeyId ?? null;
    const relevantLegs = win.legs.filter(
      (leg) => typeEnabled(leg.type) || leg.journey_id === selectedJourneyId,
    );
    // Average the route tangent across roughly six screen pixels. At national zoom this removes
    // noisy vertex-to-vertex heading changes; close up it converges to the precise local track.
    const bearingWindowM = Math.max(
      30,
      Math.min(8_000, metersPerPixel(map.getCenter().lat, map.getZoom()) * 6),
    );
    const { items, dropped } = positioned(relevantLegs, time, routes, bearingWindowM);
    const selected = selectedJourneyId === null
      ? undefined
      : items.find((item) => item.leg.journey_id === selectedJourneyId);
    const selectedTrack = selected ? trackPathById.get(selected.leg.route_id) : undefined;
    const highlightedTracks = selectedJourneyTracks.length > 0
      ? selectedJourneyTracks
      : selectedTrack
        ? [selectedTrack]
        : [];
    const selectedTrackLayer = new PathLayer<RoutePath>({
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
    overlay.setProps({
      layers: [
        trackLayer,
        selectedTrackLayer,
        stationLayer,
        trainsLayer(
          items,
          time,
          colors,
          colorMode,
          selectedJourneyId,
          map.getBearing(),
          map.getZoom(),
          (item) => void spectatePositionedTrain(item),
        ),
      ],
    });

    if (selectedJourney) {
      // Keep the camera locked to the interpolated position. Repeated easeTo calls restart an
      // animation and visibly hitch at 8×; a direct per-frame center update stays continuous.
      if (selected) map.setCenter(selected.pos);
      if (time > selectedJourney.end + 60) {
        selectedJourney = null;
        selectedJourneyTracks = [];
      }
    }

    hudDateElement.textContent = fmtHudDate(time);
    timeElement.textContent = fmtHudTime(time);
    timeElement.dateTime = new Date(time * 1000).toISOString();
    playbackIcon.textContent = clock.paused ? "▶" : "Ⅱ";
    playbackLabel.textContent = clock.paused ? "Resume playback" : "Pause playback";
    speedBadge.textContent = clock.paused ? "paused" : `${clock.speed}×`;
    countElement.textContent = `${items.length.toLocaleString()} trains${dropped ? ` · ${dropped} unplaced` : ""}`;
    requestAnimationFrame(frame);
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
