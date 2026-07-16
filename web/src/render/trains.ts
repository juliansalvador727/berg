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
import type { Leg } from "../types";

/** Colour by service class: local, regional, long-distance. */
const CLASS_COLOR: Record<string, [number, number, number]> = {
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

/** Position of a leg at simTime: fraction along the polyline, anchored at both stations. */
export function legPosition(leg: Leg, simTime: number, routes: Routes): [number, number] | null {
  // dur = 0 is quarantined at ingest, but a divide by zero here is a NaN position and an
  // invisible train — too quiet a failure to take on trust from this side of the wire.
  const frac = leg.dur > 0 ? (simTime - leg.t_dep) / leg.dur : 0;
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
  colors: [number, number, number][],
): ScatterplotLayer<PositionedLeg> {
  return new ScatterplotLayer<PositionedLeg>({
    id: "trains",
    data: items,
    getPosition: (d) => d.pos,
    getFillColor: (d) => colors[d.leg.type] ?? [200, 200, 200],
    getRadius: 3,
    radiusUnits: "pixels",
    radiusMinPixels: 2,
    pickable: true,
    // simTime alone: deck.gl caches accessor output and every position depends on it.
    updateTriggers: { getPosition: simTime },
  });
}
