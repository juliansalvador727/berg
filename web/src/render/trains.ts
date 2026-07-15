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

import type { Leg } from "../types";

/** Position of a leg at simTime: fraction along the polyline, anchored at both stations. */
export function legPosition(_leg: Leg, _simTime: number): [number, number] {
  throw new Error("M4: lerp along routes.bin polyline");
}

export function trainsLayer(_legs: Leg[], _simTime: number): unknown {
  throw new Error("M4: ScatterplotLayer over active legs");
}
