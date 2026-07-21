/**
 * DuckDB WASM lives here and only here. It never touches the main thread — a cold range
 * request is hundreds of milliseconds and would drop frames.
 *
 * Remote I/O in duckdb-wasm is sequential and single-threaded (known upstream), so the whole
 * storage layout exists to minimize the number of ranges per seek: files sorted by t_dep,
 * row groups of about an hour, per-file min/max stats for footer pruning.
 *
 * Measured against the real bucket: instantiate ~800ms, a warm windowed scrub ~40ms.
 */

import * as duckdb from "@duckdb/duckdb-wasm";
import duckdb_wasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdb_wasm_eh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import eh_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

import { MANIFEST_URL, dayFileUrl, journeyFileUrl } from "../config";
import {
  FLAG_ROUTE_FRACTION,
  JOURNEY_ID_UNAVAILABLE,
  LEG_SCHEMA_VERSION,
  type Leg,
  type Manifest,
} from "../types";

export type WorkerRequestPayload =
  | { kind: "init" }
  | { kind: "window"; simTime: number; lookahead: number }
  | { kind: "search"; query: string; simTime: number }
  | { kind: "station-board"; routeIds: number[]; simTime: number; horizon: number }
  | { kind: "prefetch"; days: string[] };

export type WorkerRequest = WorkerRequestPayload & { requestId: number };

export interface JourneySearchResult {
  journeyId: number;
  tripId: string;
  line: string;
  start: number;
  end: number;
  firstRouteId: number;
}

export interface StationBoardLeg extends Leg {
  tripId: string;
  line: string;
}

export type WorkerResponsePayload =
  | { kind: "ready"; manifest: Manifest }
  | { kind: "window"; from: number; to: number; legs: Leg[] }
  | { kind: "search-results"; day: string; results: JourneySearchResult[] }
  | { kind: "station-board"; legs: StationBoardLeg[] }
  | { kind: "error"; message: string };

export type WorkerResponse = WorkerResponsePayload & { requestId: number };

let con: duckdb.AsyncDuckDBConnection | null = null;
let manifest: Manifest | null = null;

const sqlString = (value: string): string => `'${value.replaceAll("'", "''")}'`;

function decodeLeg(row: Record<string, unknown>): Leg {
  const wireRouteId = Number(row.route_id);
  const flags = Number(row.flags);
  const hasFraction = (flags & FLAG_ROUTE_FRACTION) !== 0;
  return {
    route_id: hasFraction ? wireRouteId & 0xffff : wireRouteId,
    journey_id: Number(row.journey_id),
    route_start: hasFraction ? ((wireRouteId >>> 16) & 0xff) / 255 : 0,
    route_end: hasFraction ? (wireRouteId >>> 24) / 255 : 1,
    t_dep: Number(row.t_dep),
    dur: Number(row.dur),
    type: Number(row.type),
    delay: Number(row.delay),
    flags,
  };
}

async function init(): Promise<Manifest> {
  const bundle = await duckdb.selectBundle({
    mvp: { mainModule: duckdb_wasm, mainWorker: mvp_worker },
    eh: { mainModule: duckdb_wasm_eh, mainWorker: eh_worker },
  });
  const w = new Worker(bundle.mainWorker!, { type: "module" });
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), w);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  con = await db.connect();

  const r = await fetch(MANIFEST_URL);
  if (!r.ok) throw new Error(`manifest: HTTP ${r.status} from ${MANIFEST_URL}`);
  manifest = (await r.json()) as Manifest;
  if (manifest.schema_version > LEG_SCHEMA_VERSION) {
    throw new Error(
      `manifest schema ${manifest.schema_version} is newer than reader schema ${LEG_SCHEMA_VERSION}`,
    );
  }
  return manifest;
}

/** YYYY-MM-DD (UTC) — day files are keyed by DEPARTURE day and t_dep is UTC epoch seconds. */
export function dayKey(epochSeconds: number): string {
  return new Date(Math.floor(epochSeconds) * 1000).toISOString().slice(0, 10);
}

/**
 * The day files a [from, to] window touches.
 *
 * A leg lives in the file of its DEPARTURE day, so a window reaching back over midnight needs
 * the previous day too — that is the whole reason the client fetches day N-1 when simTime is
 * within max_leg_duration of midnight, and why legs are never duplicated across files.
 *
 * Days absent from the manifest are skipped rather than requested: the archive has 29 genuine
 * holes (2019-07-01..16 among them) and read_parquet throws on a URL that 404s — one missing
 * day would take the whole window down with it.
 */
export function filesFor(from: number, to: number, m: Manifest): string[] {
  const days = new Set<string>();
  for (let t = from; t <= to; t += 86400) days.add(dayKey(t));
  days.add(dayKey(to));
  return [...days].filter((d) => d in m.days).map(dayFileUrl);
}

/**
 * Legs departing in [simTime - max_leg_duration, simTime + lookahead].
 *
 * The lower bound is what the split rule buys: everything in flight at T departed within
 * max_leg_duration of T, so an unbounded scan becomes ~2 row groups plus the footer.
 *
 * The lookahead is what keeps rendering off the network: the main thread filters this window
 * locally every frame and only comes back when simTime nears the end of it. Querying per
 * frame would put a range request in the frame budget, which is exactly what this file exists
 * to prevent.
 */
async function windowAt(
  simTime: number,
  lookahead: number,
): Promise<{ from: number; to: number; legs: Leg[] }> {
  if (!con || !manifest) throw new Error("worker used before init");
  const from = Math.floor(simTime - manifest.max_leg_duration_s);
  const to = Math.ceil(simTime + lookahead);
  const files = filesFor(from, to, manifest);
  if (files.length === 0) return { from, to, legs: [] };

  const list = files.map((f) => `'${f}'`).join(", ");
  const journeyColumn =
    manifest.schema_version >= 3
      ? "journey_id"
      : `${JOURNEY_ID_UNAVAILABLE}::USMALLINT AS journey_id`;
  const res = await con.query(`
    SELECT route_id, ${journeyColumn}, t_dep, dur, type, delay, flags
    FROM read_parquet([${list}])
    WHERE t_dep BETWEEN ${from} AND ${to}
    ORDER BY t_dep`);

  const legs: Leg[] = new Array(res.numRows);
  for (let i = 0; i < res.numRows; i++) {
    legs[i] = decodeLeg(res.get(i)!);
  }
  return { from, to, legs };
}

/** Search journey identity/line on the active UTC day and return its first departure. */
async function searchJourneys(query: string, simTime: number): Promise<JourneySearchResult[]> {
  if (!con || !manifest) throw new Error("worker used before init");
  const day = dayKey(simTime);
  if (!(day in manifest.days)) return [];
  const needle = query.trim().toLocaleLowerCase().replaceAll(" ", "");
  if (!needle) return [];
  const match = sqlString(`%${needle}%`);
  const exact = sqlString(needle);
  const res = await con.query(`
    SELECT
      j.journey_id,
      j.trip_id,
      coalesce(j.line, '') AS line,
      min(l.t_dep) AS "start",
      max(l.t_dep + l.dur) AS "end",
      arg_min(l.route_id, l.t_dep) AS first_route_id,
      arg_min(l.flags, l.t_dep) AS first_flags,
      CASE
        WHEN replace(lower(coalesce(j.line, '')), ' ', '') = ${exact} THEN 0
        WHEN replace(lower(j.trip_id), ' ', '') = ${exact} THEN 1
        ELSE 2
      END AS rank
    FROM read_parquet(${sqlString(journeyFileUrl(day))}) j
    JOIN read_parquet(${sqlString(dayFileUrl(day))}) l USING (journey_id)
    WHERE replace(lower(j.trip_id), ' ', '') LIKE ${match}
       OR replace(lower(coalesce(j.line, '')), ' ', '') LIKE ${match}
    GROUP BY j.journey_id, j.trip_id, j.line
    ORDER BY rank, "start"
    LIMIT 24`);

  const results: JourneySearchResult[] = [];
  for (let i = 0; i < res.numRows; i++) {
    const row = res.get(i)!;
    const wireRouteId = Number(row.first_route_id);
    const flags = Number(row.first_flags);
    results.push({
      journeyId: Number(row.journey_id),
      tripId: String(row.trip_id),
      line: String(row.line),
      start: Number(row.start),
      end: Number(row.end),
      firstRouteId: (flags & FLAG_ROUTE_FRACTION) !== 0 ? wireRouteId & 0xffff : wireRouteId,
    });
  }
  return results;
}

/** Upcoming arrivals/departures for every observed route touching one station. */
async function stationBoard(
  routeIds: number[],
  simTime: number,
  horizon: number,
): Promise<StationBoardLeg[]> {
  if (!con || !manifest || routeIds.length === 0) return [];
  const from = Math.floor(simTime - 15 * 60);
  const to = Math.ceil(simTime + horizon);
  const ids = routeIds.map((id) => Math.floor(id)).join(",");
  const days = new Set<string>();
  for (let t = from; t <= to; t += 86400) days.add(dayKey(t));
  days.add(dayKey(to));

  const out: StationBoardLeg[] = [];
  for (const day of days) {
    if (!(day in manifest.days)) continue;
    const res = await con.query(`
      SELECT l.route_id, l.journey_id, l.t_dep, l.dur, l.type, l.delay, l.flags,
             coalesce(j.trip_id, '') AS trip_id, coalesce(j.line, '') AS line
      FROM read_parquet(${sqlString(dayFileUrl(day))}) l
      LEFT JOIN read_parquet(${sqlString(journeyFileUrl(day))}) j USING (journey_id)
      WHERE l.t_dep BETWEEN ${from - 86400} AND ${to}
        AND CASE WHEN (l.flags & ${FLAG_ROUTE_FRACTION}) != 0
                 THEN (l.route_id & 65535) ELSE l.route_id END IN (${ids})
      ORDER BY l.t_dep`);
    for (let i = 0; i < res.numRows; i++) {
      const row = res.get(i)!;
      out.push({ ...decodeLeg(row), tripId: String(row.trip_id), line: String(row.line) });
    }
  }
  return out;
}

/** filesAhead = ceil(speed * BUFFER_SECONDS / 86400) + 1 — 1x buffers nothing, 150x buffers 2-3. */
async function prefetch(days: string[]): Promise<void> {
  if (!con || !manifest) return;
  for (const d of days) {
    if (!(d in manifest.days)) continue;
    // WHERE false touches the footer and no row groups: warms the handle, not the data.
    await con.query(`SELECT count(*) FROM read_parquet('${dayFileUrl(d)}') WHERE false`);
  }
}

self.onmessage = async (e: MessageEvent<WorkerRequest>) => {
  const post = (m: WorkerResponsePayload) => self.postMessage({ ...m, requestId: e.data.requestId });
  try {
    switch (e.data.kind) {
      case "init":
        post({ kind: "ready", manifest: await init() });
        break;
      case "window": {
        const { from, to, legs } = await windowAt(e.data.simTime, e.data.lookahead);
        post({ kind: "window", from, to, legs });
        break;
      }
      case "search": {
        const day = dayKey(e.data.simTime);
        post({ kind: "search-results", day, results: await searchJourneys(e.data.query, e.data.simTime) });
        break;
      }
      case "station-board":
        post({
          kind: "station-board",
          legs: await stationBoard(e.data.routeIds, e.data.simTime, e.data.horizon),
        });
        break;
      case "prefetch":
        await prefetch(e.data.days);
        break;
    }
  } catch (err) {
    post({ kind: "error", message: err instanceof Error ? err.message : String(err) });
  }
};

export { prefetch, windowAt };
