"""legs/*.parquet → manifest.json, and the R2 upload path both use.

The manifest is the frontend's only source of truth for what's published — it hardcodes
schema_version and max_leg_duration_s rather than trusting the client to know them.
"""

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from berg_pipeline.constants import MAX_LEG_DURATION_S, SCHEMA_VERSION
from berg_pipeline.resources import R2Resource


R2_ENV_VARS = ("R2_ACCOUNT_ID", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")


def r2_from_env() -> R2Resource | None:
    """None (never a half-configured resource) if any credential is missing.

    Skipping the upload is what you want locally — staging the publish mirror with no
    credentials is a normal dev run. In CI it is the opposite: unset secrets would make the
    job report success having published nothing. Set BERG_REQUIRE_R2=1 there to turn a
    missing credential into a failure instead of a silent skip.
    """
    missing = [var for var in R2_ENV_VARS if not os.environ.get(var)]
    if missing:
        if os.environ.get("BERG_REQUIRE_R2") == "1":
            raise RuntimeError(
                f"BERG_REQUIRE_R2=1 but {', '.join(missing)} unset — refusing to skip the upload."
            )
        return None
    return R2Resource(
        account_id=os.environ["R2_ACCOUNT_ID"],
        bucket=os.environ["R2_BUCKET"],
        access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )


def build_manifest(legs_dir: Path, base_days: dict | None = None) -> dict:
    """One duckdb query over the glob — cheaper than stat-ing thousands of files in Python.

    base_days is what the bucket already advertises. It matters because the local mirror is
    NOT always the whole story: the monthly CI job runs on a fresh checkout holding exactly
    one month, and a manifest built from that alone would tell the frontend that every other
    year had ceased to exist. Local wins on conflict — it is the fresher build.
    """
    import duckdb

    files = sorted(legs_dir.glob("*/*/*.parquet"))
    days: dict[str, dict] = dict(base_days or {})
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
        "days": {d: days[d] for d in sorted(days)},  # merged order is arbitrary; keep it stable
        "missing_days": missing_days,
    }


def write_manifest(legs_dir: Path, out_path: Path, base_days: dict | None = None) -> dict:
    import json

    manifest = build_manifest(legs_dir, base_days=base_days)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=1))
    return manifest
