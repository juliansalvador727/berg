"""Facts: stop events → legs → per-day Parquet on R2."""

from datetime import date

import dagster as dg
from dagster_duckdb import DuckDBResource

from berg_pipeline import ingest, paths
from berg_pipeline.constants import MAX_LEG_DURATION_S
from berg_pipeline.partitions import daily_partitions, monthly_partitions

from .dimensions import dim_station
from .raw import stg_istdaten


@dg.asset(partitions_def=monthly_partitions, group_name="facts", deps=[stg_istdaten, dim_station])
def fct_legs(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Stop events → legs (one station-to-station hop of one run), one month at a time.

    ingest.build_legs does the work: LEAD over (trip, service_day) ordered by SCHEDULED time,
    validity-ranged join to dim_station, CH bbox clip, quarantine (negative durations are
    genuine source error), the MAX_LEG_DURATION_S split rule, and stable route_id/type_id
    assignment through the station_pairs and dim_train_type registries. The geometry job (M2)
    consumes station_pairs — unseen pairs get an id now and a polyline later.
    """
    month = context.partition_key[:7]
    with duckdb.get_connection() as con:
        stats = ingest.build_legs(con, month, paths.DIM_STATION_PARQUET)
        summary = ingest.month_summary(con, month)
    return dg.MaterializeResult(metadata={**stats, **{f"month_{k}": v for k, v in summary.items()}})


@dg.asset(partitions_def=daily_partitions, group_name="facts", deps=[fct_legs])
def legs_parquet(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    """One Parquet file per DEPARTURE day (UTC), at legs/YYYY/MM/DD.parquet.

    The file contract is what makes cold scrubs cheap — see infra/README.md:
      - sorted by t_dep
      - row groups ≈ 1 hour of departures (~8-10k rows; measured sweet spot 6.37 B/leg)
      - zstd; a leg belongs to the file of its departure day (no duplication — the client
        fetches day N-1 too when simTime is within MAX_LEG_DURATION_S of midnight)

    A day near a month boundary draws legs from two monthly fct_legs partitions; the export
    reads whatever is materialized, so backfills should run months in order (M3 wires the
    ordering). Zero rows is expected for the archive's 29 missing days — no file is written.

    Upload to R2 happens at M3; until then this materializes the local publish mirror.
    """
    day = date.fromisoformat(context.partition_key)
    with duckdb.get_connection() as con:
        stats = ingest.export_day(con, day, paths.legs_parquet_path(day))
    if stats["rows"] == 0:
        context.log.warning(f"{day}: no departures — archive hole or month not yet staged")
    return dg.MaterializeResult(metadata=stats)


@dg.asset(group_name="facts", deps=[legs_parquet])
def manifest() -> dg.MaterializeResult:
    """Written last, so a half-finished backfill never advertises days that aren't there.

    Contents: date range, missing-day list, file sizes, max_leg_duration, schema version.
    """
    raise NotImplementedError("M3: emit manifest.json")


@dg.asset_check(asset=fct_legs, blocking=True)
def legs_quality(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Hard invariants over every materialized month of fct_legs.

    Violations here mean the ingest SQL is wrong, not that the data is quirky — the quirky
    data is already in quarantine_legs. Band checks (legs/day vs the census) and quarantine
    ratios are reported as metadata; they gate at M3 when the backfill can compare against
    docs/archive-census.json.
    """
    with duckdb.get_connection() as con:
        ingest.create_tables(con)
        bad_dur, bad_flags, bad_null = con.execute(
            f"""SELECT
                count(*) FILTER (dur < 1 OR dur > {MAX_LEG_DURATION_S}),
                count(*) FILTER (flags NOT BETWEEN 0 AND 3),
                count(*) FILTER (t_dep IS NULL OR t_dep <= 0 OR route_id IS NULL)
            FROM fct_legs"""
        ).fetchone()
        legs_per_day = con.execute(
            """SELECT coalesce(round(avg(n)), 0) FROM (
                   SELECT count(*) AS n FROM fct_legs GROUP BY service_day)"""
        ).fetchone()[0]
        quarantined = con.execute("SELECT count(*) FROM quarantine_legs").fetchone()[0]
        total = con.execute("SELECT count(*) FROM fct_legs").fetchone()[0]

    passed = bad_dur == 0 and bad_flags == 0 and bad_null == 0
    return dg.AssetCheckResult(
        passed=bool(passed),
        metadata={
            "bad_duration": bad_dur,
            "bad_flags": bad_flags,
            "bad_nulls": bad_null,
            "avg_legs_per_service_day": legs_per_day,
            "quarantined_total": quarantined,
            "legs_total": total,
        },
    )
