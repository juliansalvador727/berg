"""Filesystem layout. Everything lives under one data root so paths don't depend on cwd.

The Dagster CLI, pytest, and the scripts all launch from different directories; deriving the
root from this file's location keeps them pointed at the same data.
"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

DATA_ROOT = Path(os.getenv("BERG_DATA_ROOT", REPO_ROOT / "data"))

# What the archive actually holds, measured from ZIP central directories (scripts/
# archive_census.py). Tracked, because ingest cross-checks extraction against it.
ARCHIVE_CENSUS = REPO_ROOT / "docs" / "archive-census.json"

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
JOURNEYS_DIR = PUBLISH_ROOT / "journeys"  # YYYY/MM/DD.parquet — click-detail sidecar
STATIC_DIR = PUBLISH_ROOT / "static"
ROUTE_PAIRS_JSON = STATIC_DIR / "route_pairs.json"


def legs_parquet_path(day) -> Path:
    return LEGS_DIR / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}.parquet"


def journeys_parquet_path(day) -> Path:
    return JOURNEYS_DIR / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}.parquet"
