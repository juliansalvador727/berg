"""legs/*.parquet → manifest.json, and the R2 upload path both use.

The manifest is the frontend's only source of truth for what's published — it hardcodes
schema_version and max_leg_duration_s rather than trusting the client to know them.
"""

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from berg_pipeline.constants import MAX_LEG_DURATION_S, SCHEMA_VERSION
from berg_pipeline.resources import R2Resource


def r2_from_env() -> R2Resource | None:
    """None (never a half-configured resource) if any credential is missing."""
    account_id = os.environ.get("R2_ACCOUNT_ID")
    bucket = os.environ.get("R2_BUCKET")
    access_key_id = os.environ.get("R2_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY")
    if not (account_id and bucket and access_key_id and secret_access_key):
        return None
    return R2Resource(
        account_id=account_id,
        bucket=bucket,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )


def build_manifest(legs_dir: Path) -> dict:
    """One duckdb query over the glob — cheaper than stat-ing thousands of files in Python."""
    import duckdb

    files = sorted(legs_dir.glob("*/*/*.parquet"))
    days: dict[str, dict] = {}
    if files:
        rows = duckdb.sql(
            f"""
            SELECT filename, count(*) AS legs
            FROM read_parquet('{legs_dir.as_posix()}/*/*/*.parquet', filename=true)
            GROUP BY filename"""
        ).fetchall()
        legs_by_path = {Path(path): legs for path, legs in rows}
        for f in files:
            day = date(int(f.parent.parent.name), int(f.parent.name), int(f.stem)).isoformat()
            days[day] = {"bytes": f.stat().st_size, "legs": legs_by_path.get(f, 0)}

    if not days:
        return {
            "schema_version": SCHEMA_VERSION,
            "max_leg_duration_s": MAX_LEG_DURATION_S,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "start": None,
            "end": None,
            "days": {},
            "missing_days": [],
        }

    start = min(days)
    end = max(days)
    missing_days = []
    d = date.fromisoformat(start)
    last = date.fromisoformat(end)
    while d <= last:
        if d.isoformat() not in days:
            missing_days.append(d.isoformat())
        d += timedelta(days=1)

    return {
        "schema_version": SCHEMA_VERSION,
        "max_leg_duration_s": MAX_LEG_DURATION_S,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "start": start,
        "end": end,
        "days": days,
        "missing_days": missing_days,
    }


def write_manifest(legs_dir: Path, out_path: Path) -> dict:
    import json

    manifest = build_manifest(legs_dir)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=1))
    return manifest
