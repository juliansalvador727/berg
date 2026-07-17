/**
 * Train rendering, in two deliberate phases.
 *
 * v1 (M4): CPU lerp → ScatterplotLayer. Peak concurrent trains in Switzerland is roughly
 * 600-1,000; a thousand lerps a frame is nothing. Ship this and watch trains move in week one.
 *
 * v2 (M6): custom instanced layer. Polylines in a texture, per-instance (route_id, t_dep, dur),
 * vertex shader does the lookup and the lerp, simTime is a uniform — zero CPU per frame.
 *
 * Not TripsLayer: it wants per-vertex timestamps, which means expanding every leg client-side
 * and throwing away the 8-byte schema that the entire storage budget depends on.
 */

import { ScatterplotLayer } from "@deck.gl/layers";

import type { Routes } from "../routes";
import { FLAG_SCHEDULED_FALLBACK, type Leg } from "../types";

type RGB = [number, number, number];

/** Colour by service class: local, regional, long-distance. */
const CLASS_COLOR: Record<string, RGB> = {
  local: [56, 189, 248],
  regional: [74, 222, 128],
  intercity: [248, 113, 113],
};

const REGIONAL = new Set(["R", "RB", "RE", "TER", "PE", "EXT"]);

/** type_id → colour, resolved once against the published category list. */
export function typeColors(types: string[]): [number, number, number][] {
  return types.map((t) => {
    if (t === "S" || t === "SN") return CLASS_COLOR.local!;
    if (REGIONAL.has(t)) return CLASS_COLOR.regional!;
    return CLASS_COLOR.intercity!;
  });
}

/**
 * A delay this large is a source error, not a late train: 0.01% of legs sit exactly on the
 * int16 clamp (-32768) because Ist-Daten sometimes reports a departure hours before its
 * schedule. Rendering "9 hours early" would be worse than admitting we do not know.
 */
export const DELAY_SANE_S = 3 * 3600;
export const isDelayKnown = (delay: number): boolean => Math.abs(delay) < DELAY_SANE_S;

/** Swiss convention: under 3 minutes counts as on time. The ramp is anchored there, not at 0. */
export const PUNCTUAL_S = 180;

/**
 * Grey means "we do not know", and it is not the same as being on time.
 *
 * A scheduled-fallback leg carries delay = 0 because ingest coalesces an unmeasured delay to
 * zero (berg_pipeline/ingest.py) — the flag, not the value, is what says the measurement is
 * missing. Colouring on the value alone would paint ~5% of every day punctual green on the
 * strength of a default. Same for the int16 clamp: a leg claiming three hours early is a
 * source error, so it reads as unknown rather than as a spectacular arrival.
 */
const DELAY_UNKNOWN: RGB = [88, 94, 108];

/** Stops interpolated in RGB: on time → 3 min → 10 min → 30 min and worse. */
const DELAY_RAMP: [number, RGB][] = [
  [0, [34, 197, 94]],
  [PUNCTUAL_S, [250, 204, 21]],
  [600, [249, 115, 22]],
  [1800, [239, 68, 68]],
];

export type ColorMode = "type" | "delay";

/** Colour for one leg's lateness, or DELAY_UNKNOWN when the delay is not a measurement. */
export function delayColor(leg: Leg): RGB {
  if ((leg.flags & FLAG_SCHEDULED_FALLBACK) !== 0) return DELAY_UNKNOWN;
  if (!isDelayKnown(leg.delay)) return DELAY_UNKNOWN;

  // Early is not a category anyone is asking about — it reads as on time.
  const d = Math.max(0, leg.delay);
  let lo = DELAY_RAMP[0]!;
  for (const stop of DELAY_RAMP) {
    if (d >= stop[0]) lo = stop;
  }
  const hi = DELAY_RAMP.find((s) => s[0] > d);
  if (!hi) return lo[1];

  const f = (d - lo[0]) / (hi[0] - lo[0]);
  return [
    Math.round(lo[1][0] + (hi[1][0] - lo[1][0]) * f),
    Math.round(lo[1][1] + (hi[1][1] - lo[1][1]) * f),
    Math.round(lo[1][2] + (hi[1][2] - lo[1][2]) * f),
  ];
}

/** Position of a leg at simTime: fraction along the polyline, anchored at both stations. */
export function legPosition(leg: Leg, simTime: number, routes: Routes): [number, number] | null {
  // dur = 0 is quarantined at ingest, but a divide by zero here is a NaN position and an
  // invisible train — too quiet a failure to take on trust from this side of the wire.
  const local = leg.dur > 0 ? (simTime - leg.t_dep) / leg.dur : 0;
  const frac = leg.route_start + local * (leg.route_end - leg.route_start);
  return routes.positionAt(leg.route_id, frac);
}

/** Legs in flight at simTime, out of a window the worker already fetched. */
export function activeAt(legs: Leg[], simTime: number): Leg[] {
  const out: Leg[] = [];
  for (const l of legs) {
    if (l.t_dep <= simTime && l.t_dep + l.dur >= simTime) out.push(l);
  }
  return out;
}

export interface PositionedLeg {
  leg: Leg;
  pos: [number, number];
}

/**
 * Active legs paired with their position, dropping any route routes.bin has never heard of.
 *
 * Dropping matters: a leg whose route_id has no polyline is not a train at [0, 0], it is a
 * train we cannot place, and defaulting the coordinate would scatter phantom trains into the
 * Gulf of Guinea. This happens for real — the geometry job is a separate, slower cadence than
 * ingest, so a backfill that discovers new station pairs runs ahead of it until it is re-run.
 * The caller is expected to surface `dropped`, not swallow it.
 */
export function positioned(
  legs: Leg[],
  simTime: number,
  routes: Routes,
): { items: PositionedLeg[]; dropped: number } {
  const items: PositionedLeg[] = [];
  let dropped = 0;
  for (const leg of legs) {
    const pos = legPosition(leg, simTime, routes);
    if (pos) items.push({ leg, pos });
    else dropped++;
  }
  return { items, dropped };
}

export function trainsLayer(
  items: PositionedLeg[],
  simTime: number,
  colors: RGB[],
  mode: ColorMode = "type",
): ScatterplotLayer<PositionedLeg> {
  return new ScatterplotLayer<PositionedLeg>({
    id: "trains",
    data: items,
    getPosition: (d) => d.pos,
    getFillColor: (d) =>
      mode === "delay" ? delayColor(d.leg) : (colors[d.leg.type] ?? [200, 200, 200]),
    getRadius: 3,
    radiusUnits: "pixels",
    radiusMinPixels: 2,
    pickable: true,
    // simTime drives every position; mode drives every colour. deck.gl caches accessor output,
    // so a mode flip without its trigger repaints nothing until the data array happens to change.
    updateTriggers: { getPosition: simTime, getFillColor: mode },
  });
}
