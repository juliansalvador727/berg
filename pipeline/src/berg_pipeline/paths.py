"""Filesystem layout. Everything lives under one data root so paths don't depend on cwd.

The Dagster CLI, pytest, and the scripts all launch from different directories; deriving the
root from this file's location keeps them pointed at the same data.
"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

DATA_ROOT = Path(os.getenv("BERG_DATA_ROOT", REPO_ROOT / "data"))

# Raw archive material, deletable at will (re-fetchable from the archive).
RAW_ISTDATEN = DATA_ROOT / "raw" / "istdaten"  # <YYYY-MM>/<YYYY-MM-DD>.csv
GTFS_CACHE = DATA_ROOT / "gtfs"

# Pipeline state.
DUCKDB_PATH = DATA_ROOT / "berg.duckdb"
DUCKDB_TMP = DATA_ROOT / "tmp"
DIM_STATION_PARQUET = DATA_ROOT / "dim_station.parquet"

# What eventually mirrors to R2 — same layout as the bucket (see infra/).
PUBLISH_ROOT = DATA_ROOT / "publish"
LEGS_DIR = PUBLISH_ROOT / "legs"  # YYYY/MM/DD.parquet
STATIC_DIR = PUBLISH_ROOT / "static"


def legs_parquet_path(day) -> Path:
    return LEGS_DIR / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}.parquet"
