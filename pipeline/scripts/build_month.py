"""Materialize one month end to end through the real Dagster assets.

raw_zip → stg_istdaten → fct_legs as one monthly run, then legs_parquet for every UTC calendar
day affected by that service month, including both boundary days. Prints the month summary the
M1 gate cares about.

Usage:
    uv run python scripts/build_month.py 2018-05 [--skip-dim]

--skip-dim trusts an existing data/dim_station.parquet instead of re-materializing the
dimension (which would re-list the GTFS archive).
"""

import argparse
import os
import sys
from datetime import timedelta

import dagster as dg

from berg_pipeline import ingest, paths, publish
from berg_pipeline.assets import dimensions, legs, raw
from berg_pipeline.constants import ARCHIVE_START
from berg_pipeline.resources import default_duckdb


def _materialize_facts(month: str, resources: dict) -> None:
    monthly_assets = [raw.raw_zip, raw.stg_istdaten, legs.fct_legs]
    result = dg.materialize(
        monthly_assets + [dimensions.dim_station.to_source_asset()],
        partition_key=f"{month}-01",
        resources=resources,
        selection=monthly_assets,
    )
    assert result.success


def _has_month_facts(month: str) -> bool:
    first, last = ingest.month_bounds(month)
    with default_duckdb().get_connection() as con:
        ingest.create_tables(con)
        return bool(
            con.execute(
                "SELECT count(*) > 0 FROM fct_legs WHERE service_day BETWEEN ? AND ?",
                [first, last],
            ).fetchone()[0]
        )


def main(month: str, skip_dim: bool) -> None:
    # Fresh CI materializes the previous month too; retaining both 11-13 GB raw directories
    # would exceed the runner disk. They are reproducible scratch once staging succeeds.
    os.environ.setdefault("BERG_DELETE_RAW", "1")
    resources = {"duckdb": default_duckdb()}

    if not skip_dim:
        result = dg.materialize([dimensions.dim_station, dimensions.stations_json])
        assert result.success
    elif not paths.DIM_STATION_PARQUET.exists():
        sys.exit(f"--skip-dim, but {paths.DIM_STATION_PARQUET} does not exist")

    # route_id/type_id are published wire ids. A fresh monthly-CI checkout has an empty DB, so
    # inherit the append-only registries before assigning ids to this month's new pairs/types.
    with default_duckdb().get_connection() as con:
        publish.bootstrap_registries(con)

    first, last = ingest.month_bounds(month)
    previous_day = first - timedelta(days=1)
    previous_month = f"{previous_day:%Y-%m}"
    if previous_day.isoformat() >= ARCHIVE_START and not _has_month_facts(previous_month):
        # A UTC boundary file contains facts from both adjacent service months. Persistent
        # backfills already have the previous month; fresh CI must materialize it explicitly.
        _materialize_facts(previous_month, resources)
    _materialize_facts(month, resources)

    # Export by UTC departure day. The first service day's 00:xx local departures land on the
    # previous UTC day, while the last service's after-midnight tail can reach the next day.
    #
    # journeys_parquet rides along rather than being a separate pass: a month whose legs ship
    # without their sidecar has clickable trains that answer nothing, and the asymmetry only
    # shows up in the UI months later. scripts/build_journeys.py exists for the backlog, not
    # for months built here.
    day_assets = [legs.legs_parquet, legs.journeys_parquet]
    departure_days = ingest.departure_days_for_month(month)
    for day in departure_days:
        r = dg.materialize(
            [*day_assets, legs.fct_legs.to_source_asset()],
            partition_key=day.isoformat(),
            resources=resources,
            selection=day_assets,
        )
        assert r.success

    # route_id → (from, to) for whatever pairs this month introduced. Cheap, and stale is
    # worse than useless: a new pair with no entry is an unnameable train.
    registry_assets = [legs.route_pairs, legs.train_types]
    r = dg.materialize(
        [*registry_assets, legs.fct_legs.to_source_asset()],
        resources=resources,
        selection=registry_assets,
    )
    assert r.success

    con = default_duckdb()
    with con.get_connection() as c:
        bad_days = ingest.validate_month_days(c, month)
        summary = ingest.month_summary(c, month)

    # The daily assets above stage locally only. Validate the complete month before a single
    # byte reaches R2, so a collapsed ingest cannot overwrite good published files and fail
    # only afterward.
    if bad_days:
        listed = ", ".join(f"{d} ({n} legs)" for d, n in bad_days[:5])
        more = f" (+{len(bad_days) - 5} more)" if len(bad_days) > 5 else ""
        # RuntimeError, not sys.exit: the backfill catches Exception to record a failed month
        # and carry on, and SystemExit would sail past it and kill the whole range.
        raise RuntimeError(
            f"{month}: {len(bad_days)} day(s) under the {ingest.MIN_LEGS_PER_DAY:,}-leg floor: "
            f"{listed}{more}. The census says these days have data, so this is a partial "
            f"ingest, not an archive hole."
        )

    upload = publish.upload_validated_outputs(departure_days)

    print(f"\n=== {month} ===")
    for k, v in summary.items():
        print(f"{k:>24}: {v}")

    files = sorted((paths.LEGS_DIR / month[:4] / month[5:7]).glob("*.parquet"))
    total_bytes = sum(f.stat().st_size for f in files)
    print(f"{'day files':>24}: {len(files)}")
    print(f"{'total MB':>24}: {total_bytes / 1e6:.1f}")
    print(f"{'files uploaded':>24}: {upload['uploaded']}")
    if summary["legs"]:
        print(f"{'bytes/leg (month)':>24}: {total_bytes / summary['legs']:.2f}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("month", help="YYYY-MM")
    p.add_argument("--skip-dim", action="store_true")
    a = p.parse_args()
    main(a.month, a.skip_dim)
