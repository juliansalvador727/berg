/**
 * M0: one real day of Swiss trains, moving.
 *
 * Main thread does map + clock + render. No DuckDB yet (M4), no route geometry yet (M2) — legs
 * are straight lines between stations. The point is that the loop is closed end to end.
 */

import { MapboxOverlay } from "@deck.gl/mapbox";
import { ScatterplotLayer } from "@deck.gl/layers";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import { Clock, SPEEDS } from "./clock";
import { INITIAL_VIEW } from "./config";
import { activeAt, parseLegs, type LegStore, type Meta } from "./m0/legs";

interface Station {
  id: number;
  name: string;
  lon: number;
  lat: number;
}

/** Colour by service class: local, regional, long-distance. */
const CLASS_COLOR: Record<string, [number, number, number]> = {
  S: [56, 189, 248],
  local: [56, 189, 248],
  regional: [74, 222, 128],
  intercity: [248, 113, 113],
};

const REGIONAL = new Set(["R", "RB", "RE", "TER", "PE", "EXT"]);

function typeColors(types: string[]): [number, number, number][] {
  return types.map((t) => {
    if (t === "S") return CLASS_COLOR.local!;
    if (REGIONAL.has(t)) return CLASS_COLOR.regional!;
    return CLASS_COLOR.intercity!;
  });
}

const fmt = (epoch: number) =>
  new Date(epoch * 1000).toLocaleTimeString("de-CH", {
    timeZone: "Europe/Zurich",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

async function main(): Promise<void> {
  const [meta, stations, legsBuf] = await Promise.all([
    fetch("/m0/meta.json").then((r) => r.json() as Promise<Meta>),
    fetch("/m0/stations.json").then((r) => r.json() as Promise<Station[]>),
    fetch("/m0/legs.bin").then((r) => r.arrayBuffer()),
  ]);
  const store: LegStore = parseLegs(legsBuf);
  const colors = typeColors(meta.types);

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

  // Start at 08:00 local — peak commute, most trains on the map.
  const clock = new Clock(meta.t_min);
  const startOfDay = meta.t_min;
  clock.seek(startOfDay + 8 * 3600 - (startOfDay % 86400));
  clock.setSpeed(60);
  clock.play();

  const ui = document.getElementById("controls")!;
  ui.innerHTML = `
    <div class="row">
      <button id="play">⏸</button>
      <span id="time">--:--:--</span>
      <span id="count" class="muted">0 trains</span>
    </div>
    <input id="scrub" type="range" min="${meta.t_min}" max="${meta.t_max}" step="1" />
    <div class="row" id="speeds"></div>
    <div class="muted small">${meta.service_day} · ${meta.legs.toLocaleString()} legs · ${stations.length.toLocaleString()} stations</div>
  `;

  const timeEl = document.getElementById("time")!;
  const countEl = document.getElementById("count")!;
  const scrub = document.getElementById("scrub") as HTMLInputElement;
  const playBtn = document.getElementById("play")!;

  const speedsEl = document.getElementById("speeds")!;
  for (const s of SPEEDS) {
    const b = document.createElement("button");
    b.textContent = `${s}×`;
    b.dataset.speed = String(s);
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

  let scrubbing = false;
  scrub.oninput = () => {
    scrubbing = true;
    clock.seek(Number(scrub.value));
  };
  scrub.onchange = () => {
    scrubbing = false;
  };

  const stationLayer = new ScatterplotLayer({
    id: "stations",
    data: stations,
    getPosition: (d: Station) => [d.lon, d.lat],
    getFillColor: [130, 130, 140, 90],
    getRadius: 2,
    radiusUnits: "pixels",
    pickable: false,
  });

  function frame() {
    const t = clock.tick();

    if (t > meta.t_max) clock.seek(meta.t_min); // loop the day

    const trains = activeAt(store, t, meta.max_leg_duration);

    overlay.setProps({
      layers: [
        stationLayer,
        new ScatterplotLayer({
          id: "trains",
          data: trains,
          getPosition: (d) => d.position,
          getFillColor: (d) => colors[d.type] ?? [200, 200, 200],
          getRadius: 3,
          radiusUnits: "pixels",
          radiusMinPixels: 2,
          updateTriggers: { getPosition: t },
        }),
      ],
    });

    timeEl.textContent = fmt(t);
    countEl.textContent = `${trains.length.toLocaleString()} trains`;
    if (!scrubbing) scrub.value = String(Math.floor(t));

    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
}

main().catch((err) => {
  console.error(err);
  document.getElementById("controls")!.textContent = `failed: ${err.message}`;
});
