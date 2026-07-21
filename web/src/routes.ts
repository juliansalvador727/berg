/**
 * routes.bin → typed arrays, no parsing.
 *
 * Struct-of-arrays, mapped straight out of the ArrayBuffer. The format is defined by
 * geometry/src/berg_geometry/binfmt.py — that file is the authority; this is a reader.
 *
 *   magic    'BRTS'
 *   u32      version (=1)
 *   u32      n_routes
 *   f64 x 4  lon_min, lat_min, lon_max, lat_max   (the quantization grid)
 *   u32[n]   route_id   (ascending — lookup is a binary search)
 *   u32[n+1] offsets    (route i owns points offsets[i]..offsets[i+1])
 *   u8[n]    flags
 *   u16[2p]  interleaved x,y
 */

export const ROUTES_MAGIC = 0x53545242; // 'BRTS' little-endian
export const ROUTES_VERSION = 1;
export const FLAG_STRAIGHT_FALLBACK = 1 << 0;

export interface RoutePath {
  routeId: number;
  path: [number, number][];
  fallback: boolean;
}

const HEADER_BYTES = 44; // magic(4) + version(4) + n(4) + bbox(32)

export class Routes {
  readonly routeIds: Uint32Array;
  readonly offsets: Uint32Array;
  readonly flags: Uint8Array;
  readonly points: Uint16Array; // interleaved x,y
  readonly bbox: [number, number, number, number];

  /** Cumulative segment length per route, built on first use — most routes are never drawn. */
  private cumCache = new Map<number, Float32Array>();

  constructor(buf: ArrayBuffer) {
    const dv = new DataView(buf);
    if (dv.getUint32(0, true) !== ROUTES_MAGIC) {
      throw new Error(`routes.bin: bad magic (got 0x${dv.getUint32(0, true).toString(16)})`);
    }
    const version = dv.getUint32(4, true);
    if (version !== ROUTES_VERSION) {
      throw new Error(`routes.bin: version ${version}, expected ${ROUTES_VERSION}`);
    }
    const n = dv.getUint32(8, true);
    this.bbox = [
      dv.getFloat64(12, true),
      dv.getFloat64(20, true),
      dv.getFloat64(28, true),
      dv.getFloat64(36, true),
    ];

    let o = HEADER_BYTES;
    this.routeIds = new Uint32Array(buf, o, n);
    o += 4 * n;
    this.offsets = new Uint32Array(buf, o, n + 1);
    o += 4 * (n + 1);
    this.flags = new Uint8Array(buf, o, n);
    o += n;

    // Points begin at 48 + 9n, which is ODD for odd n — and a Uint16Array view demands a
    // 2-byte-aligned offset. Copy in that case rather than throw; it costs one memcpy of a
    // file measured at 413 KB, and the alternative is a reader that works only for even n.
    const nPoints = this.offsets[n]!;
    this.points =
      o % 2 === 0
        ? new Uint16Array(buf, o, nPoints * 2)
        : new Uint16Array(buf.slice(o, o + nPoints * 4));

    if (this.points.length !== nPoints * 2) {
      throw new Error(`routes.bin: ${this.points.length / 2} points, offsets claim ${nPoints}`);
    }
  }

  /** Index of route_id, or -1. Ids are ascending, so binary search. */
  indexOf(routeId: number): number {
    let lo = 0;
    let hi = this.routeIds.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      const v = this.routeIds[mid]!;
      if (v === routeId) return mid;
      if (v < routeId) lo = mid + 1;
      else hi = mid - 1;
    }
    return -1;
  }

  /** Grid x,y → lon,lat. The grid is the CH bbox in uint16 — about 7 m across Switzerland. */
  private toLonLat(x: number, y: number): [number, number] {
    const [lon0, lat0, lon1, lat1] = this.bbox;
    return [(x / 65535) * (lon1 - lon0) + lon0, (y / 65535) * (lat1 - lat0) + lat0];
  }

  /**
   * Cumulative length along a route, normalized to 1.0 at the end.
   *
   * Measured in grid units, not metres: the grid is linear in lon/lat, so this is a plate
   * carrée approximation. Across Switzerland a degree of longitude is ~0.68 of a degree of
   * latitude, so segment lengths are distorted by up to ~1.5x — which would show as a train
   * easing wrongly across a leg. Correcting x by cos(lat) is one multiply, so do it.
   */
  private cumulative(i: number): Float32Array {
    const cached = this.cumCache.get(i);
    if (cached) return cached;

    const start = this.offsets[i]!;
    const end = this.offsets[i + 1]!;
    const n = end - start;
    const cum = new Float32Array(n);
    const [lon0, lat0, lon1, lat1] = this.bbox;
    // Aspect correction at the route's own latitude — routes are short, so one factor is fine.
    const midY = this.points[start * 2 + 1]! / 65535;
    const cosLat = Math.cos(((midY * (lat1 - lat0) + lat0) * Math.PI) / 180);
    const sx = ((lon1 - lon0) / 65535) * cosLat;
    const sy = (lat1 - lat0) / 65535;

    let total = 0;
    for (let k = 1; k < n; k++) {
      const dx = (this.points[(start + k) * 2]! - this.points[(start + k - 1) * 2]!) * sx;
      const dy = (this.points[(start + k) * 2 + 1]! - this.points[(start + k - 1) * 2 + 1]!) * sy;
      total += Math.hypot(dx, dy);
      cum[k] = total;
    }
    if (total > 0) for (let k = 1; k < n; k++) cum[k]! /= total;
    this.cumCache.set(i, cum);
    return cum;
  }

  /**
   * Position a fraction [0,1] along a route, by DISTANCE not vertex count.
   *
   * Vertex-count interpolation would make trains lurch: Douglas-Peucker leaves dense vertices
   * on curves and sparse ones on straights, so a train would crawl through bends and jump
   * across tangents.
   */
  positionAt(routeId: number, frac: number): [number, number] | null {
    const i = this.indexOf(routeId);
    if (i < 0) return null;

    const start = this.offsets[i]!;
    const end = this.offsets[i + 1]!;
    const n = end - start;
    if (n === 0) return null;
    if (n === 1) return this.toLonLat(this.points[start * 2]!, this.points[start * 2 + 1]!);

    const t = frac <= 0 ? 0 : frac >= 1 ? 1 : frac;
    const cum = this.cumulative(i);

    // First vertex at or past t.
    let lo = 1;
    let hi = n - 1;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (cum[mid]! < t) lo = mid + 1;
      else hi = mid;
    }
    const a = start + lo - 1;
    const b = start + lo;
    const span = cum[lo]! - cum[lo - 1]!;
    const local = span > 0 ? (t - cum[lo - 1]!) / span : 0;

    const x = this.points[a * 2]! + (this.points[b * 2]! - this.points[a * 2]!) * local;
    const y = this.points[a * 2 + 1]! + (this.points[b * 2 + 1]! - this.points[a * 2 + 1]!) * local;
    return this.toLonLat(x, y);
  }

  /** Decode every observed station-pair route once for the low-opacity network layer. */
  paths(): RoutePath[] {
    const out: RoutePath[] = new Array(this.routeIds.length);
    for (let i = 0; i < this.routeIds.length; i++) {
      const start = this.offsets[i]!;
      const end = this.offsets[i + 1]!;
      const path: [number, number][] = new Array(end - start);
      for (let j = start; j < end; j++) {
        path[j - start] = this.toLonLat(this.points[j * 2]!, this.points[j * 2 + 1]!);
      }
      out[i] = {
        routeId: this.routeIds[i]!,
        path,
        fallback: (this.flags[i]! & FLAG_STRAIGHT_FALLBACK) !== 0,
      };
    }
    return out;
  }

  get length(): number {
    return this.routeIds.length;
  }
}

export async function fetchRoutes(url: string): Promise<Routes> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`routes.bin: HTTP ${r.status} from ${url}`);
  return new Routes(await r.arrayBuffer());
}
