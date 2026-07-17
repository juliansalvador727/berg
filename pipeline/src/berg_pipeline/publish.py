"""legs/*.parquet → manifest.json, and the R2 upload path both use.

The manifest is the frontend's only source of truth for what's published — it hardcodes
schema_version and max_leg_duration_s rather than trusting the client to know them.
"""

import os
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from berg_pipeline import paths
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


def upload_validated_outputs(days: Iterable[date]) -> dict:
    """Upload one already-validated month's day artifacts, never an unchecked partial month."""
    r2 = r2_from_env()
    if r2 is None:
        return {"uploaded": 0, "upload_enabled": False}

    files: list[tuple[Path, str]] = []
    deleted: list[str] = []
    for day in days:
        for root, prefix in ((paths.LEGS_DIR, "legs"), (paths.JOURNEYS_DIR, "journeys")):
            path = root / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}.parquet"
            if path.exists():
                files.append((path, f"{prefix}/{day:%Y/%m/%d}.parquet"))
            else:
                deleted.append(f"{prefix}/{day:%Y/%m/%d}.parquet")
    if paths.ROUTE_PAIRS_JSON.exists():
        files.append((paths.ROUTE_PAIRS_JSON, "static/route_pairs.json"))
    if paths.TRAIN_TYPES_JSON.exists():
        files.append((paths.TRAIN_TYPES_JSON, "static/train_types.json"))

    client = r2.client()
    for path, key in files:
        r2.upload(path, key, client=client)
    for key in deleted:
        r2.delete(key, client=client)
    return {"uploaded": len(files), "deleted": len(deleted), "upload_enabled": True}


def bootstrap_registries(con) -> dict:
    """Seed stable wire ids from R2 before a fresh CI database builds any facts."""
    from berg_pipeline import ingest

    ingest.create_tables(con)
    pairs, types = con.execute(
        "SELECT (SELECT count(*) FROM station_pairs), (SELECT count(*) FROM dim_train_type)"
    ).fetchone()
    if pairs and types:
        return {"route_pairs_seeded": 0, "train_types_seeded": 0}

    r2 = r2_from_env()
    if r2 is None:
        return {"route_pairs_seeded": 0, "train_types_seeded": 0}
    client = r2.client()
    route_pairs = r2.get_json("static/route_pairs.json", client=client)
    train_types = r2.get_json("static/train_types.json", client=client)
    if route_pairs is None or train_types is None:
        manifest = r2.get_json("manifest.json", client=client)
        if (manifest or {}).get("days"):
            raise RuntimeError(
                "published data exists but its route/type registries are missing; refusing "
                "to assign incompatible ids from a fresh database"
            )
    return ingest.seed_registries(con, route_pairs or {}, train_types or {})


def build_manifest(
    legs_dir: Path,
    base_days: dict | None = None,
    *,
    base_schema_version: int | None = None,
    available_remote_keys: set[str] | None = None,
) -> dict:
    """One duckdb query over the glob — cheaper than stat-ing thousands of files in Python.

    base_days is what the bucket already advertises. It matters because the local mirror is
    NOT always the whole story: the monthly CI job runs on a fresh checkout holding exactly
    one month, and a manifest built from that alone would tell the frontend that every other
    year had ceased to exist. Local wins on conflict — it is the fresher build.
    """
    import duckdb

    files = sorted(legs_dir.glob("*/*/*.parquet"))
    local_days = {
        date(int(f.parent.parent.name), int(f.parent.name), int(f.stem)).isoformat() for f in files
    }
    carried_days = dict(base_days or {})
    if available_remote_keys is not None:
        carried_days = {
            day: metadata
            for day, metadata in carried_days.items()
            if f"legs/{day.replace('-', '/')}.parquet" in available_remote_keys
        }

    unreplaced = set(carried_days) - local_days
    if unreplaced and base_schema_version != SCHEMA_VERSION:
        raise RuntimeError(
            f"cannot publish schema {SCHEMA_VERSION} while {len(unreplaced)} carried day(s) "
            f"still use schema {base_schema_version}; rebuild the complete history"
        )

    days: dict[str, dict] = carried_days
    if files:
        required = {"route_id", "journey_id", "t_dep", "dur", "type", "delay", "flags"}
        schemas = duckdb.sql(
            f"""SELECT file_name, list(name)
                FROM parquet_schema('{legs_dir.as_posix()}/*/*/*.parquet')
                GROUP BY file_name"""
        ).fetchall()
        bad_schema = [Path(filename) for filename, names in schemas if not required <= set(names)]
        if bad_schema:
            raise RuntimeError(
                f"{len(bad_schema)} local leg file(s) do not match schema {SCHEMA_VERSION}; "
                f"first: {bad_schema[0]}"
            )

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


def write_manifest(
    legs_dir: Path,
    out_path: Path,
    base_days: dict | None = None,
    *,
    base_schema_version: int | None = None,
    available_remote_keys: set[str] | None = None,
) -> dict:
    import json

    manifest = build_manifest(
        legs_dir,
        base_days=base_days,
        base_schema_version=base_schema_version,
        available_remote_keys=available_remote_keys,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=1))
    return manifest
