"""Ingest: monthly Ist-Daten archive ZIP → staged stop events in DuckDB."""

import dagster as dg
from dagster_duckdb import DuckDBResource

from berg_pipeline.partitions import monthly_partitions

ARCHIVE_URL = "https://archive.opentransportdata.swiss/actual_data_archive.htm"


@dg.asset(partitions_def=monthly_partitions, group_name="ingest")
def raw_zip(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Download one monthly archive ZIP to local scratch.

    ~1.27 TB across the full archive (measured — see docs/archive-census.json), so raw ZIPs are
    deleted once stg_istdaten consumes them; never keep more than one month on disk.

    Use berg_pipeline.archive: url_for_month() knows the naming eras, and day_members() is the
    only safe way to enumerate days — member paths take six shapes, five months ship __MACOSX
    resource forks with .csv names, and 29 days across the archive are absent or 20 KB stubs.
    A missing day is expected, not a failure.
    """
    raise NotImplementedError(
        "M3: fetch the ZIP for context.partition_key via archive.url_for_month"
    )


@dg.asset(partitions_def=monthly_partitions, group_name="ingest", deps=[raw_zip])
def stg_istdaten(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Daily CSVs → one normalized stop-event table per month.

    Load-bearing details:
      - read_csv with all_varchar=true and parse explicitly. Never autodetect across ten years
        of drift: it silently typed BETRIEBSTAG as DATE on a real file.
      - Filter upper(PRODUKT_ID) = 'ZUG' first; the raw feed is mostly PostBus, and the case
        drifts ('Bus' and 'BUS' both occur).
      - Actual times are observations only when *_PROGNOSE_STATUS is in MEASURED_STATUSES.
        Otherwise fall back to scheduled and flag it.
      - NO era view is needed. Both URL series share one header; the only schema change in the
        archive is SLOID appended in 2025-11, and it is unused here. Select COLUMNS_ALL_ERAS by
        name and this spans 2018 → now. Just never SELECT *.
    """
    raise NotImplementedError("M1: normalize raw CSV to stop events")
