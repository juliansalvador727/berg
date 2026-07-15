"""Ingest: monthly Ist-Daten archive ZIP → staged stop events in DuckDB."""

import os
import shutil
import zipfile

import dagster as dg
import httpx
from dagster_duckdb import DuckDBResource

from berg_pipeline import archive, ingest, paths
from berg_pipeline.partitions import monthly_partitions


def _month_key(context: dg.AssetExecutionContext) -> str:
    return context.partition_key[:7]  # '2018-05-01' → '2018-05'


@dg.asset(partitions_def=monthly_partitions, group_name="ingest")
def raw_zip(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """One month's day CSVs extracted to local scratch.

    The full archive is ~1.27 TB (measured — docs/archive-census.json), so nothing raw is
    precious: the ZIP is deleted right after extraction, and the CSVs after staging. A month
    dir that already has CSVs is trusted as-is; delete it to force a re-download.

    day_members() is the only safe way to enumerate days — member paths take six shapes,
    five months ship __MACOSX resource forks with .csv names, and 29 days across the archive
    are absent or ~20 KB stubs. A missing day is expected, not a failure.
    """
    month = _month_key(context)
    year, mon = int(month[:4]), int(month[5:7])
    out_dir = paths.RAW_ISTDATEN / month

    existing = sorted(out_dir.glob("*.csv")) if out_dir.exists() else []
    if existing:
        return dg.MaterializeResult(
            metadata={"days": len(existing), "source": "cache", "dir": str(out_dir)}
        )

    url = archive.url_for_month(year, mon, v2=(year, mon) >= (2025, 7))
    zip_path = paths.DATA_ROOT / "raw" / "zips" / url.rsplit("/", 1)[-1]
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(f"downloading {url}")
    with httpx.stream("GET", url, follow_redirects=True, timeout=120.0) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as fh:
            for chunk in r.iter_bytes(1 << 20):
                fh.write(chunk)

    out_dir.mkdir(parents=True, exist_ok=True)
    days, skipped = [], []
    with zipfile.ZipFile(zip_path) as zf:
        for day, info in sorted(archive.day_members(zf).items()):
            if info.file_size < archive.STUB_MAX_BYTES:
                skipped.append(str(day))  # a ~20 KB stub is a hole, not a quiet day
                continue
            dest = out_dir / f"{day.isoformat()}.csv"
            with zf.open(info) as src, open(dest, "wb") as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
            days.append(str(day))
    zip_path.unlink()

    if not days:
        raise dg.Failure(f"{month}: archive ZIP contained no usable day members")
    return dg.MaterializeResult(
        metadata={"days": len(days), "stub_days_skipped": skipped, "dir": str(out_dir)}
    )


@dg.asset(partitions_def=monthly_partitions, group_name="ingest", deps=[raw_zip])
def stg_istdaten(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Daily CSVs → one normalized train stop-event table slice per month.

    The load-bearing rules live in ingest.stage_month: all_varchar (autodetect once typed
    BETRIEBSTAG as DATE), upper(PRODUKT_ID) = 'ZUG', measured = status IN MEASURED_STATUSES
    (era-free — the enum boundary is a date, not the file version), columns selected by name
    so one query spans 2018 → now.

    Set BERG_DELETE_RAW=1 to drop the month's CSVs after staging (the backfill will).
    """
    month = _month_key(context)
    csv_files = sorted((paths.RAW_ISTDATEN / month).glob("*.csv"))
    if not csv_files:
        raise dg.Failure(f"{month}: no extracted CSVs — materialize raw_zip first")

    with duckdb.get_connection() as con:
        stats = ingest.stage_month(con, month, csv_files)

    if os.getenv("BERG_DELETE_RAW") == "1":
        shutil.rmtree(paths.RAW_ISTDATEN / month)

    return dg.MaterializeResult(metadata=stats)
