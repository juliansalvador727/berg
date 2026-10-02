/**
 * DuckDB WASM lives here and only here. It never touches the main thread — a cold range
 * request is hundreds of milliseconds and would drop frames.
 *
 * Queries never read remote files. duckdb-wasm's HTTP handler re-opens a remote file with a
 * blocking, sequential HEAD on every query (~80-100 ms each, one per file), so each day file is
 * instead fetched whole, in parallel with its siblings, and registered as an in-memory buffer
 * (see the file cache below). Day files are at most 2.4 MB, so downloading them whole costs no more
 * than the old path, which ended up downloading them whole anyway.
 *
 * Windows go back as transferred typed-array columns (see legColumns.ts), never as objects.
 *
 * Measured 2026-09-30 against the live bucket, 2026-05-13, lookahead 3000 s (npm run bench):
 * warm scrub round-trip p50/p95 7/16 ms for CH alone, 25/34 ms for CH+DE+AT+IT, 36/49 ms for
 * all 8 countries (~100K legs), with zero network requests. The main thread then spends ~20 ms
 * turning the all-8 columns into Leg objects. A cold first window is bound by r2.dev download
 * speed: 0.2-0.5 s for CH, 0.6-3.2 s for all 8.
 */

import * as duckdb from "@duckdb/duckdb-wasm";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import eh_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import duckdbRuntime from "../../duckdb-runtime.json";

import {
  CATALOG_URL,
  DATA_BASE_URL,
  datasetUrl,
  dayFileUrl,
  journeyFileUrl,
} from "../config";
import {
  CATALOG_SCHEMA_VERSION,
  FLAG_ROUTE_FRACTION,
  JOURNEY_ID_UNAVAILABLE,
  LEG_SCHEMA_VERSION,
  type Catalog,
  type CatalogEntry,
  type DatasetInfo,
  type Leg,
  type Manifest,
} from "../types";
import { allocLegColumns, legColumnBuffers, type LegColumns } from "../legColumns";

export type WorkerRequestPayload =
  | { kind: "init" }
  | { kind: "window"; simTime: number; lookahead: number; datasets: number[] }
  | { kind: "search"; query: string; simTime: number; datasets: number[] }
  | { kind: "journey"; dataset: number; journeyId: number; simTime: number }
  | { kind: "journey-route"; dataset: number; journeyId: number; simTime: number }
  | {
      kind: "station-board";
      dataset: number;
      routeIds: number[];
      simTime: number;
      horizon: number;
    }
  | { kind: "prefetch"; dataset: number; days: string[] };

export type WorkerRequest = WorkerRequestPayload & { requestId: number };

export interface JourneySearchResult {
  dataset: number;
  journeyId: number;
  tripId: string;
  line: string;
  start: number;
  end: number;
  firstRouteId: number;
}

export interface StationBoardDeparture {
  journeyId: number;
  tripId: string;
  line: string;
  time: number;
  type: number;
  routeIds: number[];
}

export type WorkerResponsePayload =
  | {
      kind: "ready";
      datasets: DatasetInfo[];
      manifests: Manifest[];
      skipped: string[];
      /** The cross-border layer's path in the bucket, when the catalog lists one. */
      links: string | null;
    }
  | { kind: "window"; from: number; to: number; datasets: number[]; columns: LegColumns }
  | { kind: "search-results"; day: string; results: JourneySearchResult[] }
  | { kind: "journey-result"; result: JourneySearchResult | null }
  | { kind: "journey-route-result"; routeIds: number[] }
  | { kind: "station-board"; departures: StationBoardDeparture[] }
  | { kind: "error"; message: string };

export type WorkerResponse = WorkerResponsePayload & { requestId: number };

let db: duckdb.AsyncDuckDB | null = null;
let con: duckdb.AsyncDuckDBConnection | null = null;
let datasets: DatasetInfo[] = [];
let manifests: Manifest[] = [];

const sqlString = (value: string): string => `'${value.replaceAll("'", "''")}'`;

function decodeLeg(row: Record<string, unknown>, dataset: number): Leg {
  const wireRouteId = Number(row.route_id);
  const flags = Number(row.flags);
  const hasFraction = (flags & FLAG_ROUTE_FRACTION) !== 0;
  return {
    dataset,
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

/**
 * A result's leg columns copied into the fixed wire types. Reading whole Arrow columns is several
 * times faster than res.get(i), which materializes a row proxy per leg.
 */
function packLegColumns(res: Awaited<ReturnType<duckdb.AsyncDuckDBConnection["query"]>>): LegColumns {
  const out = allocLegColumns(res.numRows);
  for (const name of Object.keys(out) as Array<keyof LegColumns>) {
    const source = res.getChild(name)!.toArray() as ArrayLike<number | bigint>;
    const target = out[name];
    if (typeof source[0] === "bigint") for (let i = 0; i < target.length; i++) target[i] = Number(source[i]);
    else target.set(source as ArrayLike<number>);
  }
  return out;
}

/**
 * The catalog when the bucket has one, otherwise Switzerland alone at the root — exactly what
 * every pre-catalog client read, so the Swiss archive needs no rebuild or move to be listed.
 */
async function loadCatalog(): Promise<{ datasets: Record<string, CatalogEntry>; links: string | null }> {
  const swissOnly: Record<string, CatalogEntry> = {
    ch: {
      path: "",
      name: "Switzerland",
      country: "CH",
      timezone: "Europe/Zurich",
      bbox: [5.9, 45.8, 10.5, 47.9],
      leg_schema_version: LEG_SCHEMA_VERSION,
      time_semantics: "observed",
      scope: "national-passenger",
      provider: "opentransportdata.swiss (Ist-Daten)",
      license: "opentransportdata.swiss terms of use",
      license_url: "https://opentransportdata.swiss/en/terms-of-use/",
      attribution: "Source: opentransportdata.swiss",
      punctuality_threshold_s: 180,
      coverage: [{ start: "2018-01-01", end: null }],
    },
  };
  let response: Response;
  try {
    response = await fetch(CATALOG_URL);
  } catch {
    return { datasets: swissOnly, links: null };
  }
  if (response.status === 404 || response.status === 403) return { datasets: swissOnly, links: null };
  if (!response.ok) throw new Error(`catalog: HTTP ${response.status} from ${CATALOG_URL}`);
  const catalog = (await response.json()) as Catalog;
  if (catalog.catalog_schema_version > CATALOG_SCHEMA_VERSION) {
    throw new Error(
      `catalog schema ${catalog.catalog_schema_version} is newer than reader schema ${CATALOG_SCHEMA_VERSION}`,
    );
  }
  return { datasets: catalog.datasets, links: catalog.links?.path ?? null };
}

async function fetchManifest(entry: CatalogEntry): Promise<Manifest> {
  const url = datasetUrl(entry.path, "manifest.json");
  const r = await fetch(url);
  if (!r.ok) throw new Error(`manifest: HTTP ${r.status} from ${url}`);
  const m = (await r.json()) as Manifest;
  if (m.schema_version > LEG_SCHEMA_VERSION) {
    throw new Error(
      `manifest schema ${m.schema_version} is newer than reader schema ${LEG_SCHEMA_VERSION}`,
    );
  }
  return m;
}

async function init(): Promise<{
  datasets: DatasetInfo[];
  manifests: Manifest[];
  skipped: string[];
  links: string | null;
}> {
  const wasmBaseUrl = `${DATA_BASE_URL}/static/duckdb-wasm/${duckdbRuntime.version}`;
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: `${wasmBaseUrl}/${duckdbRuntime.files.mvp}`,
      mainWorker: mvp_worker,
    },
    eh: {
      mainModule: `${wasmBaseUrl}/${duckdbRuntime.files.eh}`,
      mainWorker: eh_worker,
    },
  });
  const w = new Worker(bundle.mainWorker!, { type: "module" });
  db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), w);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  con = await db.connect();
  // The first parquet query pays a one-off engine warm-up of ~600ms. Pay it on a tiny file while
  // the catalog and manifests download, not on the first window.
  const warm = (async () => {
    await db!.registerEmptyFileBuffer("warmup.parquet");
    await con!.query(`COPY (SELECT 1::UINTEGER AS t_dep) TO 'warmup.parquet' (FORMAT parquet)`);
    await con!.query(`SELECT count(*) FROM read_parquet('warmup.parquet') WHERE t_dep BETWEEN 0 AND 2`);
  })().catch((err: unknown) => console.warn("parquet warm-up failed", err));

  // Switzerland first, so it is always dataset 0 and a Swiss-only session behaves exactly as
  // before. A broken foreign manifest costs that country, never the Swiss archive.
  const catalog = await loadCatalog();
  const entries = Object.entries(catalog.datasets).sort(([a], [b]) =>
    a === "ch" ? -1 : b === "ch" ? 1 : a.localeCompare(b),
  );
  const loaded = await Promise.allSettled(entries.map(([, entry]) => fetchManifest(entry)));
  const skipped: string[] = [];
  datasets = [];
  manifests = [];
  loaded.forEach((result, i) => {
    const [id, entry] = entries[i]!;
    if (result.status === "rejected") {
      if (id === "ch") throw result.reason;
      console.warn(`dataset ${id} unavailable`, result.reason);
      skipped.push(id);
      return;
    }
    datasets.push({ ...entry, id, index: datasets.length });
    manifests.push(result.value);
  });
  await warm;
  return { datasets, manifests, skipped, links: catalog.links };
}

/**
 * Day files held in DuckDB's memory, keyed by URL.
 *
 * read_parquet on an https URL makes duckdb-wasm's JS file handler reopen the file with a
 * blocking HEAD on every query, one file at a time, whatever the HTTP/object cache settings say.
 * Day files are small (largest leg file 2.4 MB, journeys ~90 KB) and immutable within a session,
 * so each is fetched whole once, in parallel with its siblings, and queried from a registered
 * buffer: a warm query never touches the network.
 */
interface CachedFile {
  name: string;
  bytes: number;
  lastUsed: number;
  pins: number;
  ready: Promise<string>;
}
const files = new Map<string, CachedFile>();
/** Worst case in use is 8 countries x 2 days x (legs + journeys), about 45 MB. The rest is prefetch. */
const CACHE_BUDGET_BYTES = 96 * 1024 * 1024;
let cachedBytes = 0;

/** The bucket-relative path, so two countries' days can never clash. Switzerland lives at the root. */
function localName(url: string): string {
  const path = url.startsWith(`${DATA_BASE_URL}/`)
    ? url.slice(DATA_BASE_URL.length + 1)
    : new URL(url).pathname.slice(1);
  return path.startsWith("legs/") || path.startsWith("journeys/") ? `ch/${path}` : path;
}

async function download(url: string, entry: CachedFile): Promise<string> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`day file ${url}: HTTP ${response.status}`);
  const bytes = new Uint8Array(await response.arrayBuffer());
  entry.bytes = bytes.byteLength; // registerFileBuffer transfers the buffer, detaching it
  await db!.registerFileBuffer(entry.name, bytes);
  cachedBytes += entry.bytes;
  evict();
  return entry.name;
}

/** Pin one file, fetching it if needed. Concurrent callers share the one download. */
function ensure(url: string): Promise<string> {
  if (!db) throw new Error("worker used before init");
  let entry = files.get(url);
  if (!entry) {
    const created: CachedFile = {
      name: localName(url),
      bytes: 0,
      lastUsed: 0,
      pins: 0,
      ready: Promise.resolve(""),
    };
    created.ready = download(url, created).catch((err: unknown) => {
      // Never cache a failure: the next query tries again.
      if (files.get(url) === created) files.delete(url);
      throw err;
    });
    files.set(url, created);
    entry = created;
  }
  entry.pins++;
  entry.lastUsed = performance.now();
  return entry.ready;
}

/** Pin every file and fetch the missing ones in parallel. Pair with release() in a finally. */
async function ensureAll(urls: string[]): Promise<string[]> {
  const settled = await Promise.allSettled(urls.map(ensure));
  const failed = settled.find((result) => result.status === "rejected");
  if (failed) {
    release(urls);
    throw failed.reason;
  }
  return settled.map((result) => (result as PromiseFulfilledResult<string>).value);
}

function release(urls: string[]): void {
  for (const url of urls) {
    const entry = files.get(url);
    if (entry && entry.pins > 0) entry.pins--;
  }
  evict();
}

/** Drop least-recently-used unpinned files until the cache is back under budget. */
function evict(): void {
  if (cachedBytes <= CACHE_BUDGET_BYTES) return;
  const idle = [...files.entries()]
    .filter(([, entry]) => entry.pins === 0 && entry.bytes > 0)
    .sort(([, a], [, b]) => a.lastUsed - b.lastUsed);
  for (const [url, entry] of idle) {
    if (cachedBytes <= CACHE_BUDGET_BYTES) break;
    files.delete(url);
    cachedBytes -= entry.bytes;
    void db!.dropFile(entry.name).catch((err: unknown) => console.warn(`dropFile ${entry.name}`, err));
  }
}

/** Run fn with the files pinned in memory, under their local names in the same order. */
async function withFiles<T>(urls: string[], fn: (names: string[]) => Promise<T>): Promise<T> {
  const names = await ensureAll(urls);
  try {
    return await fn(names);
  } finally {
    release(urls);
  }
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
 * holes (2019-07-01..16 among them) and a day file that 404s fails its query — one missing
 * day would take the whole window down with it.
 */
export function filesFor(from: number, to: number, m: Manifest, path = ""): string[] {
  const days = new Set<string>();
  for (let t = from; t <= to; t += 86400) days.add(dayKey(t));
  days.add(dayKey(to));
  return [...days].filter((d) => d in m.days).map((d) => dayFileUrl(path, d));
}

function dataset(index: number): { info: DatasetInfo; manifest: Manifest } {
  const info = datasets[index];
  const manifest = manifests[index];
  if (!con || !info || !manifest) throw new Error(`worker has no dataset ${index}`);
  return { info, manifest };
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
  selected: number[],
): Promise<{ from: number; to: number; datasets: number[]; columns: LegColumns }> {
  if (!con) throw new Error("worker used before init");
  const wanted = selected.filter((index) => datasets[index] !== undefined);
  const lower = Math.max(0, ...wanted.map((index) => manifests[index]!.max_leg_duration_s));
  const from = Math.floor(simTime - lower);
  const to = Math.ceil(simTime + lookahead);

  // One UNION ALL over every selected country, each file list tagged with a constant dataset
  // index. Countries only ever share the query, never an id space.
  // Every country's files are fetched as one parallel batch before any SQL runs.
  const perDataset = wanted.map((index) => {
    const { info, manifest } = dataset(index);
    return { index, manifest, urls: filesFor(from, to, manifest, info.path) };
  });
  const urls = perDataset.flatMap(({ urls }) => urls);
  if (urls.length === 0) return { from, to, datasets: wanted, columns: allocLegColumns(0) };
  const res = await withFiles(urls, (names) => {
    const parts: string[] = [];
    let next = 0;
    for (const { index, manifest, urls } of perDataset) {
      const local = names.slice(next, (next += urls.length));
      if (local.length === 0) continue;
      const list = local.map(sqlString).join(", ");
      const journeyColumn =
        manifest.schema_version >= 3
          ? "journey_id"
          : `${JOURNEY_ID_UNAVAILABLE}::USMALLINT AS journey_id`;
      parts.push(`
      SELECT ${index}::UTINYINT AS dataset, route_id, ${journeyColumn}, t_dep, dur, type, delay, flags
      FROM read_parquet([${list}])
      WHERE t_dep BETWEEN ${from} AND ${to}`);
    }
    return con!.query(`${parts.join(" UNION ALL ")} ORDER BY t_dep`);
  });

  return { from, to, datasets: wanted, columns: packLegColumns(res) };
}

/** Search every selected country's journeys on the active UTC day. */
async function searchJourneys(
  query: string,
  simTime: number,
  selected: number[],
): Promise<JourneySearchResult[]> {
  const ranked: Array<JourneySearchResult & { rank: number }> = [];
  for (const index of selected) {
    if (datasets[index] === undefined) continue;
    ranked.push(...(await searchDataset(index, query, simTime)));
  }
  return ranked
    .sort((a, b) => a.rank - b.rank || a.start - b.start)
    .slice(0, 24)
    .map(({ rank: _rank, ...result }) => result);
}

async function searchDataset(
  index: number,
  query: string,
  simTime: number,
): Promise<Array<JourneySearchResult & { rank: number }>> {
  const { info, manifest } = dataset(index);
  const day = dayKey(simTime);
  if (!(day in manifest.days)) return [];
  const needle = query.trim().toLocaleLowerCase().replaceAll(" ", "");
  if (!needle) return [];
  const match = sqlString(`%${needle}%`);
  const exact = sqlString(needle);
  const res = await withFiles([journeyFileUrl(info.path, day), dayFileUrl(info.path, day)], ([journeys, legs]) => con!.query(`
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
    FROM read_parquet(${sqlString(journeys!)}) j
    JOIN read_parquet(${sqlString(legs!)}) l USING (journey_id)
    WHERE replace(lower(j.trip_id), ' ', '') LIKE ${match}
       OR replace(lower(coalesce(j.line, '')), ' ', '') LIKE ${match}
    GROUP BY j.journey_id, j.trip_id, j.line
    ORDER BY rank, "start"
    LIMIT 24`));

  const results: Array<JourneySearchResult & { rank: number }> = [];
  for (let i = 0; i < res.numRows; i++) {
    const row = res.get(i)!;
    const wireRouteId = Number(row.first_route_id);
    const flags = Number(row.first_flags);
    results.push({
      dataset: index,
      journeyId: Number(row.journey_id),
      tripId: String(row.trip_id),
      line: String(row.line),
      start: Number(row.start),
      end: Number(row.end),
      firstRouteId: (flags & FLAG_ROUTE_FRACTION) !== 0 ? wireRouteId & 0xffff : wireRouteId,
      rank: Number(row.rank),
    });
  }
  return results;
}

/** Resolve one compact daily journey ID to the identity shown by the spectator UI. */
async function journeyById(
  index: number,
  journeyId: number,
  simTime: number,
): Promise<JourneySearchResult | null> {
  const { info, manifest } = dataset(index);
  const day = dayKey(simTime);
  if (!(day in manifest.days)) return null;
  const id = Math.max(0, Math.min(0xffff, Math.floor(journeyId)));
  const res = await withFiles([journeyFileUrl(info.path, day), dayFileUrl(info.path, day)], ([journeys, legs]) => con!.query(`
    SELECT
      j.journey_id,
      j.trip_id,
      coalesce(j.line, '') AS line,
      min(l.t_dep) AS "start",
      max(l.t_dep + l.dur) AS "end",
      arg_min(l.route_id, l.t_dep) AS first_route_id,
      arg_min(l.flags, l.t_dep) AS first_flags
    FROM read_parquet(${sqlString(journeys!)}) j
    JOIN read_parquet(${sqlString(legs!)}) l USING (journey_id)
    WHERE j.journey_id = ${id}
    GROUP BY j.journey_id, j.trip_id, j.line
    LIMIT 1`));
  if (res.numRows === 0) return null;
  const row = res.get(0)!;
  const wireRouteId = Number(row.first_route_id);
  const flags = Number(row.first_flags);
  return {
    dataset: index,
    journeyId: Number(row.journey_id),
    tripId: String(row.trip_id),
    line: String(row.line),
    start: Number(row.start),
    end: Number(row.end),
    firstRouteId: (flags & FLAG_ROUTE_FRACTION) !== 0 ? wireRouteId & 0xffff : wireRouteId,
  };
}

/** Every observed station-pair geometry traversed by one journey, in travel order. */
async function journeyRoute(index: number, journeyId: number, simTime: number): Promise<number[]> {
  const { info, manifest } = dataset(index);
  const day = dayKey(simTime);
  if (!(day in manifest.days)) return [];
  const id = Math.max(0, Math.min(0xffff, Math.floor(journeyId)));
  const res = await withFiles([dayFileUrl(info.path, day)], ([legs]) => con!.query(`
    SELECT route_id, flags
    FROM read_parquet(${sqlString(legs!)})
    WHERE journey_id = ${id}
    ORDER BY t_dep`));
  const routeIds: number[] = [];
  for (let i = 0; i < res.numRows; i++) {
    const row = res.get(i)!;
    const wireRouteId = Number(row.route_id);
    const flags = Number(row.flags);
    const routeId = (flags & FLAG_ROUTE_FRACTION) !== 0 ? wireRouteId & 0xffff : wireRouteId;
    if (routeIds[routeIds.length - 1] !== routeId) routeIds.push(routeId);
  }
  return routeIds;
}

/** Recent and upcoming departures, including each train's remaining observed route. */
async function stationBoard(
  index: number,
  routeIds: number[],
  simTime: number,
  horizon: number,
): Promise<StationBoardDeparture[]> {
  if (routeIds.length === 0) return [];
  const { info, manifest } = dataset(index);
  const from = Math.floor(simTime - 15 * 60);
  const to = Math.ceil(simTime + horizon);
  const ids = routeIds.map((id) => Math.floor(id)).join(",");
  const days = new Set<string>();
  for (let t = from; t <= to; t += 86400) days.add(dayKey(t));
  days.add(dayKey(to));

  const out: StationBoardDeparture[] = [];
  for (const day of days) {
    if (!(day in manifest.days)) continue;
    out.push(
      ...(await withFiles([dayFileUrl(info.path, day), journeyFileUrl(info.path, day)], ([legs, journeys]) =>
        boardDay(index, legs!, journeys!, from, to, ids),
      )),
    );
  }
  return out.sort((a, b) => a.time - b.time);
}

/** One departure day of a station board, read from its two pinned local files. */
async function boardDay(
  index: number,
  legsFile: string,
  journeysFile: string,
  from: number,
  to: number,
  ids: string,
): Promise<StationBoardDeparture[]> {
  const out: StationBoardDeparture[] = [];
  const candidates = await con!.query(`
    SELECT l.route_id, l.journey_id, l.t_dep, l.dur, l.type, l.delay, l.flags,
           coalesce(j.trip_id, '') AS trip_id, coalesce(j.line, '') AS line
    FROM read_parquet(${sqlString(legsFile)}) l
    LEFT JOIN read_parquet(${sqlString(journeysFile)}) j USING (journey_id)
    WHERE l.t_dep BETWEEN ${from} AND ${to}
      AND CASE WHEN (l.flags & ${FLAG_ROUTE_FRACTION}) != 0
               THEN (l.route_id & 65535) ELSE l.route_id END IN (${ids})
    ORDER BY l.t_dep`);

  const departures: Array<{ leg: Leg; tripId: string; line: string }> = [];
  for (let i = 0; i < candidates.numRows; i++) {
    const row = candidates.get(i)!;
    const leg = decodeLeg(row, index);
    // Long legs are split into route fractions. Only the first fraction actually departs
    // from the station represented by this route ID.
    if (leg.route_start !== 0) continue;
    departures.push({ leg, tripId: String(row.trip_id), line: String(row.line) });
  }
  if (departures.length === 0) return [];

  const journeyIds = [...new Set(departures.map(({ leg }) => leg.journey_id))];
  const journeyLegRows = await con!.query(`
    SELECT route_id, journey_id, t_dep, dur, type, delay, flags
    FROM read_parquet(${sqlString(legsFile)})
    WHERE journey_id IN (${journeyIds.join(",")})
    ORDER BY journey_id, t_dep, route_id`);
  const legsByJourney = new Map<number, Leg[]>();
  for (let i = 0; i < journeyLegRows.numRows; i++) {
    const leg = decodeLeg(journeyLegRows.get(i)!, index);
    legsByJourney.set(leg.journey_id, [...(legsByJourney.get(leg.journey_id) ?? []), leg]);
  }

  for (const departure of departures) {
    const journeyLegs = legsByJourney.get(departure.leg.journey_id) ?? [departure.leg];
    const start = journeyLegs.findIndex(
      (leg) =>
        leg.t_dep === departure.leg.t_dep &&
        leg.route_id === departure.leg.route_id &&
        leg.route_start === departure.leg.route_start,
    );
    const remainingLegs = journeyLegs.slice(Math.max(0, start));
    const remainingRouteIds: number[] = [];
    for (const leg of remainingLegs) {
      if (remainingRouteIds[remainingRouteIds.length - 1] !== leg.route_id) {
        remainingRouteIds.push(leg.route_id);
      }
    }
    out.push({
      journeyId: departure.leg.journey_id,
      tripId: departure.tripId,
      line: departure.line,
      time: departure.leg.t_dep,
      type: departure.leg.type,
      routeIds: remainingRouteIds,
    });
  }
  return out;
}

/**
 * Pull whole day files into the cache ahead of need, with no SQL. Fire-and-forget: the files are
 * released at once, so they are the first to go if the budget runs short.
 */
async function prefetch(index: number, days: string[]): Promise<void> {
  if (!db || !datasets[index]) return;
  const { info, manifest } = dataset(index);
  const urls = days
    .filter((d) => d in manifest.days)
    .flatMap((d) => [dayFileUrl(info.path, d), journeyFileUrl(info.path, d)]);
  if (urls.length === 0) return;
  await ensureAll(urls);
  release(urls);
}

self.onmessage = async (e: MessageEvent<WorkerRequest>) => {
  const post = (m: WorkerResponsePayload, transfer: Transferable[] = []) =>
    self.postMessage({ ...m, requestId: e.data.requestId }, { transfer });
  try {
    switch (e.data.kind) {
      case "init":
        post({ kind: "ready", ...(await init()) });
        break;
      case "window": {
        const result = await windowAt(e.data.simTime, e.data.lookahead, e.data.datasets);
        post({ kind: "window", ...result }, legColumnBuffers(result.columns));
        break;
      }
      case "search": {
        const day = dayKey(e.data.simTime);
        post({
          kind: "search-results",
          day,
          results: await searchJourneys(e.data.query, e.data.simTime, e.data.datasets),
        });
        break;
      }
      case "journey":
        post({
          kind: "journey-result",
          result: await journeyById(e.data.dataset, e.data.journeyId, e.data.simTime),
        });
        break;
      case "journey-route":
        post({
          kind: "journey-route-result",
          routeIds: await journeyRoute(e.data.dataset, e.data.journeyId, e.data.simTime),
        });
        break;
      case "station-board":
        post({
          kind: "station-board",
          departures: await stationBoard(
            e.data.dataset,
            e.data.routeIds,
            e.data.simTime,
            e.data.horizon,
          ),
        });
        break;
      case "prefetch":
        // No reply: a failed prefetch only means the next query fetches the file itself.
        prefetch(e.data.dataset, e.data.days).catch((err: unknown) => console.warn("prefetch failed", err));
        break;
    }
  } catch (err) {
    post({ kind: "error", message: err instanceof Error ? err.message : String(err) });
  }
};

export { prefetch, windowAt };
