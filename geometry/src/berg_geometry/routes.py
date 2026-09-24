"""Shortest paths per observed station pair → simplified, quantized polylines."""

from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
from shapely import LineString

from berg_geometry.binfmt import FLAG_STRAIGHT_FALLBACK, Bbox, quantize
from berg_geometry.graph import RailGraph, StationGraph
from berg_geometry.proj import CH_BBOX, seg_lengths_m, to_meters

# Douglas-Peucker tolerance. At ~10 m, a polyline is visually identical at max zoom and a
# fraction of the vertices. Applied in flat meter space so the tolerance means meters.
SIMPLIFY_TOLERANCE_M = 10.0

# When the shortest rail path is BOTH 4× the straight line AND >10 km longer, the direct
# track is missing from the extract (Bossonnens→Palézieux while the TPF line is disused in
# OSM; Neuhausen→Rafz whose real line runs through Germany, clipped from the CH extract) and
# a straight line beats drawing a train on a 55 km detour it never took. Both thresholds
# must hold: mountain switchbacks legitimately exceed 4× on short hops (Chernex→Chamby),
# and long alpine passes exceed +10 km at healthy ratios.
DETOUR_RATIO = 4.0
DETOUR_EXCESS_M = 10_000.0


@dataclass(frozen=True)
class Pair:
    route_id: int
    from_bpuic: int
    to_bpuic: int
    from_lon: float
    from_lat: float
    to_lon: float
    to_lat: float


def observed_pairs(duckdb_path: Path, dim_station_parquet: Path) -> list[Pair]:
    """Every (from, to) pair trains actually ran, with each station's CURRENT coordinates.

    The registry is append-only, so this is the full decade-so-far, not one month. Current
    coordinates are the right ones to route with: the track doesn't move when a station is
    renamed, and a station that physically moved did so by meters.
    """
    con = duckdb.connect(str(duckdb_path), read_only=True)
    rows = con.execute(
        f"""
        WITH current AS (
            SELECT bpuic, lon, lat FROM '{dim_station_parquet.as_posix()}'
            QUALIFY row_number() OVER (PARTITION BY bpuic ORDER BY valid_from DESC) = 1
        )
        SELECT p.route_id, p.from_bpuic, p.to_bpuic, f.lon, f.lat, t.lon, t.lat
        FROM station_pairs p
        JOIN current f ON f.bpuic = p.from_bpuic
        JOIN current t ON t.bpuic = p.to_bpuic
        ORDER BY p.from_bpuic, p.to_bpuic"""
    ).fetchall()
    return [Pair(*r) for r in rows]


def simplify_polyline(lons: np.ndarray, lats: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Douglas-Peucker in meter space; endpoints always survive."""
    if len(lons) <= 2:
        return lons, lats
    x, y = to_meters(lons, lats)
    line = LineString(np.column_stack([x, y])).simplify(SIMPLIFY_TOLERANCE_M)
    xs, ys = np.asarray(line.coords).T
    from berg_geometry.proj import M_PER_DEG_LAT, M_PER_DEG_LON

    return xs / M_PER_DEG_LON, ys / M_PER_DEG_LAT


def route_all(
    graph: RailGraph, pairs: list[Pair], bbox: Bbox = CH_BBOX
) -> tuple[dict[int, tuple[np.ndarray, int]], dict]:
    """One polyline per pair. Unroutable or unsnappable pairs get a straight line and a flag —
    a train cutting a corner beats a missing train.

    Returns (routes for write_routes_bin, report dict).
    """
    # One virtual node per distinct station, wired to every nearby track.
    stations: dict[int, tuple[float, float]] = {}
    for p in pairs:
        stations.setdefault(p.from_bpuic, (p.from_lon, p.from_lat))
        stations.setdefault(p.to_bpuic, (p.to_lon, p.to_lat))
    sg = StationGraph.build(graph, stations)

    # Group by source station: one Dijkstra per unique source.
    by_src: dict[int, list[tuple[Pair, int]]] = {}
    fallback: list[tuple[Pair, str]] = []
    for p in pairs:
        s, t = sg.vidx.get(p.from_bpuic), sg.vidx.get(p.to_bpuic)
        if s is None or t is None:
            fallback.append((p, "unsnappable"))
        else:
            by_src.setdefault(s, []).append((p, t))

    routes: dict[int, tuple[np.ndarray, int]] = {}
    ratios: list[float] = []
    done = 0
    for src, group in by_src.items():
        paths = sg.paths_from(src, [t for _, t in group])
        for p, t in group:
            path = paths[t]
            if path is None:
                fallback.append((p, "unreachable"))
                continue
            # The endpoints ARE the virtual station nodes, so consecutive legs meet exactly
            # at the station dot by construction.
            lons, lats = sg.lons[path], sg.lats[path]
            path_m = float(seg_lengths_m(lons, lats).sum())
            straight_m = float(
                seg_lengths_m(
                    np.asarray([p.from_lon, p.to_lon]), np.asarray([p.from_lat, p.to_lat])
                ).sum()
            )
            if (
                straight_m > 0
                and path_m / straight_m > DETOUR_RATIO
                and path_m - straight_m > DETOUR_EXCESS_M
            ):
                fallback.append((p, "absurd_detour"))
                continue
            if straight_m > 0:
                ratios.append(path_m / straight_m)
            lons, lats = simplify_polyline(lons, lats)
            routes[p.route_id] = (quantize(lons, lats, bbox), 0)
        done += 1
        if done % 200 == 0:
            print(f"  routed {done}/{len(by_src)} sources", flush=True)

    for p, _reason in fallback:
        xy = quantize(
            np.asarray([p.from_lon, p.to_lon]), np.asarray([p.from_lat, p.to_lat]), bbox
        )
        routes[p.route_id] = (xy, FLAG_STRAIGHT_FALLBACK)

    r = np.asarray(ratios)
    dists = list(sg.snap_dist.values())
    report = {
        "pairs": len(pairs),
        "stations": len(stations),
        "stations_snapped": len(sg.vidx),
        "snap_dist_p50_m": round(float(np.median(dists)), 1),
        "snap_dist_p95_m": round(float(np.percentile(dists, 95)), 1),
        "unsnapped_stations": sorted(b for b in stations if b not in sg.vidx)[:20],
        "fallback": {
            reason: sum(1 for _, r_ in fallback if r_ == reason)
            for reason in {r_ for _, r_ in fallback}
        },
        "ratio_p50": round(float(np.median(r)), 3) if len(r) else None,
        "ratio_p99": round(float(np.percentile(r, 99)), 3) if len(r) else None,
        "ratio_max": round(float(r.max()), 3) if len(r) else None,
    }
    return routes, report
