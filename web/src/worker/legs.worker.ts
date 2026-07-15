/**
 * DuckDB WASM lives here and only here. It never touches the main thread — a cold range
 * request is hundreds of milliseconds and would drop frames.
 *
 * Remote I/O in duckdb-wasm is sequential and single-threaded (known upstream), so the whole
 * storage layout exists to minimize the number of ranges per seek: files sorted by t_dep,
 * row groups of about an hour, per-file min/max stats for footer pruning.
 */

import type { Leg } from "../types";

export type WorkerRequest =
  | { kind: "init" }
  | { kind: "activeAt"; simTime: number }
  | { kind: "prefetch"; days: string[] };

export type WorkerResponse =
  | { kind: "ready" }
  | { kind: "active"; simTime: number; legs: Leg[] };

/**
 * Every leg in flight at T departed in [T - max_leg_duration, T]. That invariant is the whole
 * reason the split rule exists: it turns an unbounded scan into ~2 row groups plus the footer.
 *
 *   SELECT * FROM legs WHERE t_dep BETWEEN :t - :maxdur AND :t
 *   -- in flight iff t_dep + dur >= :t
 *
 * The client fetches day N, and day N-1 too when simTime is within max_leg_duration of
 * midnight — legs belong to the file of their departure day and are not duplicated.
 */
async function activeAt(_simTime: number): Promise<Leg[]> {
  throw new Error("M4: query registered day files");
}

/** filesAhead = ceil(speed * BUFFER_SECONDS / 86400) + 1 — 1x buffers nothing, 150x buffers 2-3. */
async function prefetch(_days: string[]): Promise<void> {
  throw new Error("M4: register day files ahead of the clock");
}

self.onmessage = async (_e: MessageEvent<WorkerRequest>) => {
  throw new Error("M4: wire up init/activeAt/prefetch");
};

export { activeAt, prefetch };
