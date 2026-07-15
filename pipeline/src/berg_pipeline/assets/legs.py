"""Facts: stop events → legs → per-day Parquet on R2."""

import dagster as dg
from dagster_duckdb import DuckDBResource

from berg_pipeline.partitions import daily_partitions, monthly_partitions
from berg_pipeline.resources import R2Resource


@dg.asset(partitions_def=monthly_partitions, group_name="facts")
def fct_legs(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    dim_station,
    dim_route,
) -> dg.MaterializeResult:
    """Stop events → legs (one station-to-station hop of one run).

    Steps, all inside DuckDB:
      1. LEAD(...) OVER (PARTITION BY fahrt_bezeichner, service_date ORDER BY abfahrtszeit)
         turns consecutive stop events into legs.
      2. Validity-ranged join to dim_station; resolve route_id via dim_route. Unseen station
         pairs are enqueued for the geometry job rather than failing the run.
      3. Apply the MAX_LEG_DURATION_S split rule, setting FLAG_SYNTHETIC_SPLIT.
    """
    raise NotImplementedError("M1: stop events → legs")


@dg.asset(partitions_def=daily_partitions, group_name="facts", deps=[fct_legs])
def legs_parquet(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    r2: R2Resource,
) -> dg.MaterializeResult:
    """One Parquet file per service day, uploaded to legs/YYYY/MM/DD.parquet.

    The file contract is what makes cold scrubs cheap — see infra/README.md:
      - sorted by t_dep
      - row groups ≈ 1 hour of departures (the load-bearing knob: row group size = bytes
        downloaded per scrub; measure and iterate)
      - zstd; dictionary on route_id/type; delta on t_dep
      - a leg belongs to the file of its DEPARTURE day (no duplication — the client fetches
        day N-1 too when simTime is within MAX_LEG_DURATION_S of midnight)

    Measure bytes/leg here at M1. The budget is <= 8; re-plan if > 10.
    """
    raise NotImplementedError("M1: COPY TO PARQUET, then upload")


@dg.asset(group_name="facts", deps=[legs_parquet])
def manifest(r2: R2Resource) -> dg.MaterializeResult:
    """Written last, so a half-finished backfill never advertises days that aren't there.

    Contents: date range, file sizes, max_leg_duration, schema version.
    """
    raise NotImplementedError("M3: emit manifest.json")


@dg.asset_check(asset=fct_legs, blocking=True)
def legs_quality(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Row counts vs raw, % measured vs fallback, unmatched BPUICs, max delay sanity,
    legs/day within the expected band (~200-225k)."""
    raise NotImplementedError("M1: quality checks")
