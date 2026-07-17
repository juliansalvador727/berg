/**
 * M4: the whole published archive, replayed from R2.
 *
 * No data ships with the app. DuckDB WASM range-queries one day file at a time in a worker,
 * routes.bin supplies the track geometry, and every position is a lerp along a polyline at
 * simTime — so scrub precision is free regardless of how coarse the files are.
 */

import { MapboxOverlay } from "@deck.gl/mapbox";
import { ScatterplotLayer } from "@deck.gl/layers";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import { Clock, SPEEDS } from "./clock";
import {
  BUFFER_SECONDS,
  INITIAL_VIEW,
  ROUTE_PAIRS_URL,
  ROUTES_URL,
  STATIONS_URL,
  TRAIN_TYPES_URL,
} from "./config";
import { fetchRoutes, type Routes } from "./routes";
import {
  activeAt,
  type ColorMode,
  delayColor,
  positioned,
  PUNCTUAL_S,
  trainsLayer,
  typeColors,
} from "./render/trains";
import { FLAG_SCHEDULED_FALLBACK, type Leg, type Manifest } from "./types";
import type { WorkerRequest, WorkerResponse } from "./worker/legs.worker";

interface Station {
  id: number;
  name: string;
  lon: number;
  lat: number;
}

/** Refetch when simTime is within this much of the window's end — never mid-frame. */
const REFETCH_MARGIN_S = 60;

const fmtClock = (epoch: number) =>
  new Date(epoch * 1000).toLocaleString("de-CH", {
    timeZone: "Europe/Zurich",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

function ask(worker: Worker, req: WorkerRequest): Promise<WorkerResponse> {
  return new Promise((resolve, reject) => {
    const onMsg = (e: MessageEvent<WorkerResponse>) => {
      if (e.data.kind === "error") {
        worker.removeEventListener("message", onMsg);
        reject(new Error(e.data.message));
        return;
      }
      worker.removeEventListener("message", onMsg);
      resolve(e.data);
    };
    worker.addEventListener("message", onMsg);
    worker.postMessage(req);
  });
}

async function main(): Promise<void> {
  const status = document.getElementById("controls")!;
  status.textContent = "starting DuckDB…";

  const worker = new Worker(new URL("./worker/legs.worker.ts", import.meta.url), {
    type: "module",
  });

  const ready = await ask(worker, { kind: "init" });
  if (ready.kind !== "ready") throw new Error("worker did not become ready");
  const manifest: Manifest = ready.manifest;
  if (!manifest.start || !manifest.end) throw new Error("manifest advertises no days");

  status.textContent = "loading geometry…";
  const [routes, allStations, typeMap, routePairs] = await Promise.all([
    fetchRoutes(ROUTES_URL) as Promise<Routes>,
    fetch(STATIONS_URL).then((r) => r.json() as Promise<Station[]>),
    fetch(TRAIN_TYPES_URL).then((r) => r.json() as Promise<Record<string, string>>),
    fetch(ROUTE_PAIRS_URL).then((r) => r.json() as Promise<Record<string, [number, number]>>),
  ]);

  // stations.json is the whole dimension — 32k stops, most of them bus stops that no train
  // ever calls at, and drawn raw they bury the trains in grey. The stations worth showing are
  // exactly the ones a leg ends at, which route_pairs already enumerates.
  const served = new Set<number>();
  for (const [from, to] of Object.values(routePairs)) {
    served.add(from);
    served.add(to);
  }
  const stations = allStations.filter((s) => served.has(s.id));

  // type_id is a dense uint8; the published map is keyed by its decimal string.
  const maxType = Math.max(...Object.keys(typeMap).map(Number));
  const types = Array.from({ length: maxType + 1 }, (_, i) => typeMap[String(i)] ?? "?");
  const colors = typeColors(types);

  const map = new maplibregl.Map({
    container: "map",
    style: "https://demotiles.maplibre.org/style.json", // M0 placeholder; Protomaps at M6
    center: [INITIAL_VIEW.longitude, INITIAL_VIEW.latitude],
    zoom: INITIAL_VIEW.zoom,
    attributionControl: { compact: true },
  });
  await map.once("load");
  const overlay = new MapboxOverlay({ interleaved: false, layers: [] });
  map.addControl(overlay);

  // Scrub spans the whole published archive; start at 08:00 local on the first full day.
  const days = Object.keys(manifest.days).sort();
  const tMin = Date.parse(`${days[0]}T00:00:00Z`) / 1000;
  const tMax = Date.parse(`${days[days.length - 1]}T23:59:59Z`) / 1000;
  const clock = new Clock(tMin + 6 * 3600);
  clock.setSpeed(60);
  clock.play();

  status.innerHTML = `
    <div class="row">
      <button id="play">⏸</button>
      <span id="time">--</span>
      <span id="count" class="muted">0 trains</span>
    </div>
    <input id="scrub" type="range" min="${tMin}" max="${tMax}" step="1" />
    <div class="row" id="speeds"></div>
    <div class="row">
      <span class="muted small">colour</span>
      <button id="mode-type" class="on">type</button>
      <button id="mode-delay">delay</button>
      <span id="legend" class="small"></span>
    </div>
    <div class="muted small">
      ${days.length.toLocaleString()} days · ${manifest.start} → ${manifest.end}
      · ${routes.length.toLocaleString()} routes · ${stations.length.toLocaleString()} stations
      <span id="dropped"></span>
    </div>`;

  const timeEl = document.getElementById("time")!;
  const countEl = document.getElementById("count")!;
  const droppedEl = document.getElementById("dropped")!;
  const scrub = document.getElementById("scrub") as HTMLInputElement;
  const playBtn = document.getElementById("play")!;

  const speedsEl = document.getElementById("speeds")!;
  for (const s of SPEEDS) {
    const b = document.createElement("button");
    b.textContent = `${s}×`;
    b.onclick = () => {
      clock.setSpeed(s);
      for (const el of speedsEl.children) el.classList.toggle("on", el === b);
    };
    if (s === 60) b.classList.add("on");
    speedsEl.appendChild(b);
  }
  playBtn.onclick = () => {
    if (clock.paused) clock.play();
    else clock.pause();
    playBtn.textContent = clock.paused ? "▶" : "⏸";
  };

  // The legend asks the same functions the layer does, so a ramp edit cannot leave the key
  // describing colours the map stopped using.
  const swatch = (c: [number, number, number], label: string) =>
    `<span class="key"><i style="background:rgb(${c[0]},${c[1]},${c[2]})"></i>${label}</span>`;
  const legendEl = document.getElementById("legend")!;
  const modeBtns: Record<ColorMode, HTMLElement> = {
    type: document.getElementById("mode-type")!,
    delay: document.getElementById("mode-delay")!,
  };
  let colorMode: ColorMode = "type";

  function renderLegend() {
    const leg = (delay: number, flags = 0): Leg => ({
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
    legendEl.innerHTML =
      colorMode === "delay"
        ? [
            swatch(delayColor(leg(0)), "on time"),
            swatch(delayColor(leg(PUNCTUAL_S)), "3 min"),
            swatch(delayColor(leg(600)), "10 min"),
            swatch(delayColor(leg(1800)), "30 min+"),
            swatch(delayColor(leg(0, FLAG_SCHEDULED_FALLBACK)), "unmeasured"),
          ].join("")
        : [
            swatch(typeColors(["S"])[0]!, "local"),
            swatch(typeColors(["R"])[0]!, "regional"),
            swatch(typeColors(["IC"])[0]!, "long-distance"),
          ].join("");
  }

  for (const m of ["type", "delay"] as ColorMode[]) {
    modeBtns[m].onclick = () => {
      colorMode = m;
      for (const [k, el] of Object.entries(modeBtns)) el.classList.toggle("on", k === m);
      renderLegend();
    };
  }
  renderLegend();

  let scrubbing = false;
  scrub.oninput = () => {
    scrubbing = true;
    clock.seek(Number(scrub.value));
  };
  scrub.onchange = () => {
    scrubbing = false;
  };

  const stationLayer = new ScatterplotLayer<Station>({
    id: "stations",
    data: stations,
    getPosition: (d) => [d.lon, d.lat],
    getFillColor: [130, 130, 140, 90],
    getRadius: 2,
    radiusUnits: "pixels",
    pickable: false,
  });

  // The window the worker last delivered. Frames read this; only a refetch replaces it.
  let win: { from: number; to: number; legs: Leg[] } = { from: 0, to: -1, legs: [] };
  let inFlight = false;

  /** Buffer scales with speed: at 600x a second of wall clock is ten minutes of simulation. */
  const lookahead = () => Math.max(BUFFER_SECONDS, BUFFER_SECONDS * (clock.speed / 60));

  async function refill(t: number): Promise<void> {
    if (inFlight) return;
    inFlight = true;
    try {
      const res = await ask(worker, { kind: "window", simTime: t, lookahead: lookahead() });
      if (res.kind === "window") win = res;
    } catch (e) {
      console.error("window fetch failed", e);
    } finally {
      inFlight = false;
    }
  }
  await refill(clock.simTime);

  function frame() {
    const t = clock.tick();
    if (t > tMax) clock.seek(tMin);

    // Outside the buffered window, or close enough to its edge to be worth topping up.
    if (t < win.from || t > win.to - REFETCH_MARGIN_S) void refill(t);

    const live = activeAt(win.legs, t);
    const { items, dropped } = positioned(live, t, routes);
    overlay.setProps({ layers: [stationLayer, trainsLayer(items, t, colors, colorMode)] });

    timeEl.textContent = fmtClock(t);
    countEl.textContent = `${items.length.toLocaleString()} trains`;
    // Loud, not silent: routes.bin lagging the pipeline means trains we cannot draw.
    droppedEl.textContent = dropped > 0 ? ` · ⚠ ${dropped} without geometry` : "";
    if (!scrubbing) scrub.value = String(Math.floor(t));
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
}

main().catch((err) => {
  console.error(err);
  document.getElementById("controls")!.textContent = `failed: ${err.message}`;
});
