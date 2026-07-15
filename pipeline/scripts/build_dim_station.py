"""CLI for the dim_station SCD2 build — the logic lives in berg_pipeline.dim_station.

Usage:
    uv run python scripts/build_dim_station.py [--cache ../data/gtfs] [--out ../data/dim_station.parquet]
"""

import argparse
from pathlib import Path

from berg_pipeline import paths
from berg_pipeline.dim_station import build_full

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--cache", type=Path, default=paths.GTFS_CACHE)
    p.add_argument("--out", type=Path, default=paths.DIM_STATION_PARQUET)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--limit", type=int, default=None, help="only the N most recent snapshots")
    a = p.parse_args()

    stats = build_full(a.cache, a.out, a.workers, a.limit)
    print(f"built from {stats['snapshots_usable']}/{stats['snapshots_listed']} snapshots")
