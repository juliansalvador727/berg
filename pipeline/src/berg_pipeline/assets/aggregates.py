"""Build-time aggregates: the dashboards you get without a server."""

import dagster as dg
from dagster_duckdb import DuckDBResource

from berg_pipeline.resources import R2Resource


@dg.asset(group_name="aggregates")
def aggregates_json(duckdb: DuckDBResource, r2: R2Resource, legs_parquet) -> dg.MaterializeResult:
    """Small JSONs on R2, one fetch each at runtime.

    Delay percentiles by year/month, top-N delayed stations and corridors, trains-per-hour
    profile, punctuality (< PUNCTUALITY_THRESHOLD_S) over time.
    """
    raise NotImplementedError("M6: build-time aggregate queries → static/aggregates/*.json")
