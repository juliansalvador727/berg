/**
 * A window of legs as one typed array per column: what the worker sends to the main thread.
 *
 * Posting ~100K Leg objects meant a structured clone on both sides, about 100 ms in the worker
 * and 50 ms of main-thread stall per refill. Typed arrays are transferred, not copied, and
 * building the objects from them on the main thread is a single cheap loop.
 */

import { FLAG_ROUTE_FRACTION, type Leg } from "./types";

export interface LegColumns {
  dataset: Uint8Array;
  route_id: Uint32Array; // on the wire: base route id, or packed with a route fraction
  journey_id: Uint16Array;
  t_dep: Uint32Array;
  dur: Uint16Array;
  type: Uint8Array;
  delay: Int16Array;
  flags: Uint8Array;
}

export function allocLegColumns(n: number): LegColumns {
  return {
    dataset: new Uint8Array(n),
    route_id: new Uint32Array(n),
    journey_id: new Uint16Array(n),
    t_dep: new Uint32Array(n),
    dur: new Uint16Array(n),
    type: new Uint8Array(n),
    delay: new Int16Array(n),
    flags: new Uint8Array(n),
  };
}

/** The buffers to list as transferables when posting the columns. */
export const legColumnBuffers = (c: LegColumns): ArrayBuffer[] =>
  Object.values(c).map((column: { buffer: ArrayBufferLike }) => column.buffer as ArrayBuffer);

export function legsFromColumns(c: LegColumns): Leg[] {
  const n = c.t_dep.length;
  const legs: Leg[] = new Array(n);
  for (let i = 0; i < n; i++) {
    const wireRouteId = c.route_id[i]!;
    const flags = c.flags[i]!;
    const hasFraction = (flags & FLAG_ROUTE_FRACTION) !== 0;
    legs[i] = {
      dataset: c.dataset[i]!,
      route_id: hasFraction ? wireRouteId & 0xffff : wireRouteId,
      journey_id: c.journey_id[i]!,
      route_start: hasFraction ? ((wireRouteId >>> 16) & 0xff) / 255 : 0,
      route_end: hasFraction ? (wireRouteId >>> 24) / 255 : 1,
      t_dep: c.t_dep[i]!,
      dur: c.dur[i]!,
      type: c.type[i]!,
      delay: c.delay[i]!,
      flags,
    };
  }
  return legs;
}
