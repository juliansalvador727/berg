"""Materialize one month end to end through the real Dagster assets.

raw_zip → stg_istdaten → fct_legs as one monthly run, then legs_parquet for every calendar
day whose file gains rows (a service day's night trains depart after midnight, so day N+1
gets a few of day N's legs). Prints the month summary the M1 gate cares about.

Usage:
    uv run python scripts/build_month.py 2018-05 [--skip-dim]

--skip-dim trusts an existing data/dim_station.parquet instead of re-materializing the
dimension (which would re-list the GTFS archive).
"""

import argparse
import sys
from datetime import timedelta

import dagster as dg

from berg_pipeline import ingest, paths
from berg_pipeline.assets import dimensions, legs, raw
from berg_pipeline.resources import default_duckdb


def main(month: str, skip_dim: bool) -> None:
    resources = {"duckdb": default_duckdb()}
    monthly_assets = [raw.raw_zip, raw.stg_istdaten, legs.fct_legs]

    if not skip_dim:
        result = dg.materialize([dimensions.dim_station, dimensions.stations_json])
        assert result.success
    elif not paths.DIM_STATION_PARQUET.exists():
        sys.exit(f"--skip-dim, but {paths.DIM_STATION_PARQUET} does not exist")

    result = dg.materialize(
        monthly_assets + [dimensions.dim_station.to_source_asset()],
        partition_key=f"{month}-01",
        resources=resources,
        selection=monthly_assets,
    )
    assert result.success

    # Export by departure day: the month's days plus the first day of the next month, which
    # receives the last night's post-midnight departures.
    first, last = ingest.month_bounds(month)
    for day in [*ingest.days_in_month(month), last + timedelta(days=1)]:
        r = dg.materialize(
            [legs.legs_parquet, legs.fct_legs.to_source_asset()],
            partition_key=day.isoformat(),
            resources=resources,
            selection=[legs.legs_parquet],
        )
        assert r.success

    con = default_duckdb()
    with con.get_connection() as c:
        summary = ingest.month_summary(c, month)
    print(f"\n=== {month} ===")
    for k, v in summary.items():
        print(f"{k:>24}: {v}")

    files = sorted((paths.LEGS_DIR / month[:4] / month[5:7]).glob("*.parquet"))
    total_bytes = sum(f.stat().st_size for f in files)
    print(f"{'day files':>24}: {len(files)}")
    print(f"{'total MB':>24}: {total_bytes / 1e6:.1f}")
    if summary["legs"]:
        print(f"{'bytes/leg (month)':>24}: {total_bytes / summary['legs']:.2f}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("month", help="YYYY-MM")
    p.add_argument("--skip-dim", action="store_true")
    a = p.parse_args()
    main(a.month, a.skip_dim)
