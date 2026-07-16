"""Ingest: monthly Ist-Daten archive ZIP → staged stop events in DuckDB."""

import os
import shutil
import zipfile

import dagster as dg
import httpx
from dagster_duckdb import DuckDBResource

from berg_pipeline import archive, ingest, paths
from berg_pipeline.constants import V2_FIRST_FULL_MONTH
from berg_pipeline.partitions import monthly_partitions


def _month_key(context: dg.AssetExecutionContext) -> str:
    return context.partition_key[:7]  # '2018-05-01' → '2018-05'


def _assert_complete(month: str, got: int) -> None:
    """Fail loudly when a month yields fewer days than the census says it holds.

    This is the guard that was missing: picking the v2 series for 2025-07 silently dropped
    12 real days (v2 launched mid-month) and the month still reported success. A short month
    must never pass quietly — it is indistinguishable from a good one downstream.

    The archive's genuine holes are already in the census's usable_days, so this compares
    against what the ZIP really has, not against the calendar.
    """
    expected = archive.expected_usable_days(month)
    if expected is not None and got < expected:
        raise dg.Failure(
            f"{month}: {got} usable day CSVs but the census expects {expected}. "
            f"Refusing to stage a short month — check the series (v1 vs v2, see "
            f"V2_FIRST_FULL_MONTH) and whether a previous run left a partial dir."
        )


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
        _assert_complete(month, len(existing))  # a killed run leaves a partial dir
        return dg.MaterializeResult(
            metadata={"days": len(existing), "source": "cache", "dir": str(out_dir)}
        )

    url = archive.url_for_month(year, mon, v2=(year, mon) >= V2_FIRST_FULL_MONTH)
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
    _assert_complete(month, len(days))
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
