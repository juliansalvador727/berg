"""The whole job: PBF + station_pairs → routes.bin (+ a JSON report next to it).

Usage:
    uv run python -m berg_geometry.build \
        --pbf ../data/raw/switzerland-latest.osm.pbf \
        --db ../data/berg.duckdb \
        --dim ../data/dim_station.parquet \
        --out ../data/publish/static/routes.bin
"""

import argparse
import json
import time
from pathlib import Path

from berg_geometry.binfmt import write_routes_bin
from berg_geometry.graph import RailGraph
from berg_geometry.pbf import load_rail_network
from berg_geometry.routes import observed_pairs, route_all


def main(pbf: Path, db: Path, dim: Path, out: Path) -> dict:
    t0 = time.monotonic()
    print(f"loading rail network from {pbf.name} ...", flush=True)
    net = load_rail_network(pbf)
    print(f"  {net.n_nodes:,} nodes, {len(net.edges):,} edges ({time.monotonic() - t0:.0f}s)")

    graph = RailGraph.build(net)
    pairs = observed_pairs(db, dim)
    print(f"routing {len(pairs):,} station pairs ...", flush=True)
    routes, report = route_all(graph, pairs)

    stats = write_routes_bin(routes, out)
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
    a = p.parse_args()
    main(a.pbf, a.db, a.dim, a.out)
