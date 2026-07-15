"""Ingest: monthly Ist-Daten archive ZIP → staged stop events in DuckDB."""

import dagster as dg
from dagster_duckdb import DuckDBResource

from berg_pipeline.partitions import monthly_partitions

ARCHIVE_URL = "https://archive.opentransportdata.swiss/actual_data_archive.htm"


@dg.asset(partitions_def=monthly_partitions, group_name="ingest")
def raw_zip(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Download one monthly archive ZIP to local scratch.

    ~1.8 TB across the full archive, so raw ZIPs are deleted once stg_istdaten consumes them —
    never keep more than one month on disk.
    """
    raise NotImplementedError("M3: fetch the ZIP for context.partition_key from ARCHIVE_URL")


@dg.asset(partitions_def=monthly_partitions, group_name="ingest", deps=[raw_zip])
def stg_istdaten(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Daily CSVs → one normalized stop-event table per month.

    Load-bearing details:
      - read_csv with an EXPLICIT schema. Never autodetect across ten years of drift.
      - Filter PRODUKT_ID = 'Zug' first; the raw feed is mostly PostBus.
      - Actual times are only observations when *_PROGNOSE_STATUS says so ('GESCHAETZT' in v1;
        verify the v2 enum against the cookbook). Otherwise fall back to scheduled and flag it.
      - v1 and v2 get one normalizing view each; this asset picks by partition date.
    """
    raise NotImplementedError("M1: normalize raw CSV to stop events")
