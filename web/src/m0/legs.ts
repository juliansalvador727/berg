/**
 * M0 leg store: struct-of-arrays over one binary blob, mapped to typed arrays with no parsing.
 *
 * Straight-line geometry and inlined coordinates — both go away at M2/M4 once routes.bin and
 * DuckDB WASM exist. What survives is the access pattern: legs sorted by t_dep, and the
 * "in flight at T ⇒ departed in [T - maxDur, T]" invariant that turns a scan into a slice.
 */

export interface LegStore {
  n: number;
  tDep: Uint32Array;
  dur: Uint16Array;
  /** [fromLon, fromLat, toLon, toLat] per leg. */
  coords: Float32Array;
  type: Uint8Array;
}

export interface Meta {
  legs: number;
  types: string[];
  t_min: number;
  t_max: number;
  max_leg_duration: number;
  service_day: string;
}

export function parseLegs(buf: ArrayBuffer): LegStore {
  const magic = new TextDecoder().decode(new Uint8Array(buf, 0, 4));
  if (magic !== "BERG") throw new Error(`bad magic: ${magic}`);
  const head = new DataView(buf, 4, 8);
  const version = head.getUint32(0, true);
  if (version !== 1) throw new Error(`unsupported format version ${version}`);
  const n = head.getUint32(4, true);

  let o = 12;
  const tDep = new Uint32Array(buf.slice(o, (o += n * 4)));
  const dur = new Uint16Array(buf.slice(o, (o += n * 2)));
  const coords = new Float32Array(buf.slice(o, (o += n * 16)));
  const type = new Uint8Array(buf.slice(o, o + n));

  return { n, tDep, dur, coords, type };
}

/** First index with tDep >= t. */
function lowerBound(tDep: Uint32Array, t: number): number {
  let lo = 0;
  let hi = tDep.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (tDep[mid]! < t) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

export interface ActiveTrain {
  position: [number, number];
  type: number;
}

/**
 * Trains in flight at simTime.
 *
 * The whole scan is the slice [lowerBound(t - maxDur), lowerBound(t + 1)) because legs are
 * sorted by t_dep and none lasts longer than maxDur. That is the same bound the real scrub
 * query uses against Parquet row groups — here it just costs two binary searches.
 */
export function activeAt(store: LegStore, simTime: number, maxDur: number): ActiveTrain[] {
  const t = Math.floor(simTime);
  const start = lowerBound(store.tDep, t - maxDur);
  const end = lowerBound(store.tDep, t + 1);
  const out: ActiveTrain[] = [];

  for (let i = start; i < end; i++) {
    const dep = store.tDep[i]!;
    const d = store.dur[i]!;
    if (dep + d < t) continue; // departed inside the window but already arrived

    const f = d === 0 ? 0 : (simTime - dep) / d;
    const c = i * 4;
    const lon = store.coords[c]! + (store.coords[c + 2]! - store.coords[c]!) * f;
    const lat = store.coords[c + 1]! + (store.coords[c + 3]! - store.coords[c + 1]!) * f;
    out.push({ position: [lon, lat], type: store.type[i]! });
  }
  return out;
}
