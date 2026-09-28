"""The whole job: PBF + station_pairs → routes.bin (+ a JSON report next to it).

Usage:
    uv run python -m berg_geometry.build \
        --pbf ../data/raw/switzerland-latest.osm.pbf \
        --db ../data/berg.duckdb \
        --dim ../data/dim_station.parquet \
        --out ../data/publish/static/routes.bin

Any other dataset passes --fit-bbox: the quantization grid and projection are fitted to the
stations it serves instead of Switzerland's (see binfmt.py). The Swiss file keeps CH_BBOX.

    uv run python -m berg_geometry.build --fit-bbox \
        --pbf ../data/raw/finland-latest.osm.pbf \
        --db ../data/datasets/fi/berg.duckdb \
        --dim ../data/datasets/fi/dim_station.parquet \
        --out ../data/datasets/fi/publish/static/routes.bin
"""

import argparse
import json
import time
from pathlib import Path

from berg_geometry import proj
from berg_geometry.binfmt import write_routes_bin
from berg_geometry.graph import RailGraph
from berg_geometry.pbf import load_rail_network
from berg_geometry.proj import CH_BBOX
from berg_geometry.routes import Pair, observed_pairs, route_all

# Margin around the served stations: polylines leave the straight station-to-station box on
# curves, and anything outside the grid clamps.
FIT_MARGIN_DEG = 0.1


def fitted_bbox(pairs: list[Pair]) -> tuple[float, float, float, float]:
    lons = [c for p in pairs for c in (p.from_lon, p.to_lon)]
    lats = [c for p in pairs for c in (p.from_lat, p.to_lat)]
    return (
        round(min(lons) - FIT_MARGIN_DEG, 4),
        round(min(lats) - FIT_MARGIN_DEG, 4),
        round(max(lons) + FIT_MARGIN_DEG, 4),
        round(max(lats) + FIT_MARGIN_DEG, 4),
    )


def main(pbf: Path, db: Path, dim: Path, out: Path, fit_bbox: bool = False) -> dict:
    t0 = time.monotonic()
    pairs = observed_pairs(db, dim)
    bbox = fitted_bbox(pairs) if fit_bbox else CH_BBOX
    # Projected before anything is measured: graph weights, snapping and simplify all use it.
    proj.set_reference_latitude((bbox[1] + bbox[3]) / 2)
    print(f"loading rail network from {pbf.name} ...", flush=True)
    net = load_rail_network(pbf)
    print(f"  {net.n_nodes:,} nodes, {len(net.edges):,} edges ({time.monotonic() - t0:.0f}s)")

    graph = RailGraph.build(net)
    print(f"routing {len(pairs):,} station pairs in bbox {bbox} ...", flush=True)
    routes, report = route_all(graph, pairs, bbox)

    stats = write_routes_bin(routes, out, bbox)
    report["bbox"] = list(bbox)
    report |= stats
    report["elapsed_s"] = round(time.monotonic() - t0)
    report_path = out.with_suffix(".report.json")
    report_path.write_text(json.dumps(report, indent=2))

    for k, v in report.items():
        print(f"{k:>22}: {v}")
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--pbf", type=Path, required=True)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--dim", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--fit-bbox", action="store_true", help="grid fitted to served stations")
    a = p.parse_args()
    main(a.pbf, a.db, a.dim, a.out, a.fit_bbox)
