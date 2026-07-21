"""Upload the local publish mirror to R2 — no recompute, just bytes.

data/publish is a byte-for-byte mirror of the bucket root, so this is a plain sync of
everything already staged there by the pipeline. Use it after a backfill that ran without
credentials, or to resume one that died partway.

Three rules the ordering encodes:

  - The manifest is regenerated from what is actually on disk, so it can never advertise a
    day that was never staged.
  - Static registries and geometry upload before facts. New route ids can safely coexist with
    old facts, while new facts served against old routes.bin would make trains disappear.
  - The manifest uploads after every required object — a half-finished sync leaves the site
    on the old contract. Obsolete managed keys are deleted only after that commit point.

Files whose content checksum matches the bucket are skipped. Single-part objects verify via
ETag; multipart objects use SHA-256 metadata written by this pipeline.

Usage:
    set -a; source .env; set +a
    uv run python scripts/sync_publish.py [--dry-run] [--delete]
"""

import argparse
import json
import sys
from pathlib import Path

from berg_pipeline import paths, publish, routesbin

MANIFEST_KEY = "manifest.json"
MANAGED_PREFIXES = ("static/", "journeys/", "legs/")
REQUIRED_STATIC = (
    "route_pairs.json",
    "routes.bin",
    "stations.json",
    "train_types.json",
)


def upload_order(key: str) -> tuple[int, str]:
    """A safe in-place deployment order; manifest.json is handled separately."""
    if key.startswith("static/"):
        return (0, key)
    if key.startswith("journeys/"):
        return (1, key)
    if key.startswith("legs/"):
        return (2, key)
    return (3, key)


def managed_stale_keys(remote_keys: set[str], local_keys: set[str]) -> list[str]:
    """Remote mirror leftovers we own and may remove with explicit --delete.

    Unknown top-level keys are deliberately preserved. The bucket may grow unrelated content
    later, and a data sync must never infer authority over it merely because it is absent from
    data/publish.
    """
    return sorted(key for key in remote_keys - local_keys if key.startswith(MANAGED_PREFIXES))


def validate_local_mirror() -> dict[str, int]:
    """Refuse a coherent-looking upload whose local artifacts do not agree.

    The geometry job runs separately from the backfill, and journey sidecars are optional at
    the filesystem level. Both failure modes otherwise survive until a user opens the map.
    """
    missing_static = [name for name in REQUIRED_STATIC if not (paths.STATIC_DIR / name).is_file()]
    if missing_static:
        raise RuntimeError(f"required static artifacts missing: {', '.join(missing_static)}")

    leg_days = {
        p.relative_to(paths.LEGS_DIR).as_posix() for p in paths.LEGS_DIR.glob("*/*/*.parquet")
    }
    journey_days = {
        p.relative_to(paths.JOURNEYS_DIR).as_posix()
        for p in paths.JOURNEYS_DIR.glob("*/*/*.parquet")
    }
    if leg_days != journey_days:
        legs_only = sorted(leg_days - journey_days)
        journeys_only = sorted(journey_days - leg_days)
        raise RuntimeError(
            "legs/journeys mirror mismatch: "
            f"{len(legs_only)} legs-only (first: {legs_only[:1]}), "
            f"{len(journeys_only)} journeys-only (first: {journeys_only[:1]})"
        )

    pairs = {int(route_id) for route_id in json.loads(paths.ROUTE_PAIRS_JSON.read_text())}
    header = routesbin.read_header(paths.STATIC_DIR / "routes.bin")
    geometry_ids = set(header.route_ids)
    missing_routes = pairs - geometry_ids
    if missing_routes:
        raise RuntimeError(
            f"routes.bin is stale: {len(missing_routes)} registered route ids have no geometry "
            f"(first: {sorted(missing_routes)[:5]}). Run the geometry job before syncing."
        )

    return {
        "leg_days": len(leg_days),
        "journey_days": len(journey_days),
        "registered_routes": len(pairs),
        "geometry_routes": len(geometry_ids),
    }


def _payload() -> tuple[list[Path], dict[Path, str]]:
    files = [p for p in paths.PUBLISH_ROOT.rglob("*") if p.is_file()]
    keys = {p: p.relative_to(paths.PUBLISH_ROOT).as_posix() for p in files}
    payload = sorted(
        (p for p in files if keys[p] != MANIFEST_KEY),
        key=lambda p: upload_order(keys[p]),
    )
    return payload, keys


def _print_pending(pending: list[Path], keys: dict[Path, str]) -> None:
    by_group: dict[str, list[str]] = {}
    for path in pending:
        key = keys[path]
        group = key.split("/", 1)[0]
        by_group.setdefault(group, []).append(key)
    for group, group_keys in by_group.items():
        print(f"  {group}: {len(group_keys)} to upload")
        for key in group_keys[:3]:
            print(f"    {key}")
        if len(group_keys) > 3:
            print(f"    ... and {len(group_keys) - 3} more")


def main(dry_run: bool, delete: bool) -> int:
    try:
        validated = validate_local_mirror()
    except (OSError, ValueError, RuntimeError) as error:
        print(f"preflight failed: {error}", file=sys.stderr)
        return 1
    print(
        f"preflight: {validated['leg_days']} leg/journey days, "
        f"{validated['registered_routes']} registered routes, "
        f"{validated['geometry_routes']} geometries"
    )

    manifest_path = paths.PUBLISH_ROOT / MANIFEST_KEY
    # Dry-run must not rewrite generated_at or otherwise touch the local mirror. The manifest
    # always uploads on a real run because its generated_at is intentionally fresh.
    data = (
        publish.build_manifest(paths.LEGS_DIR)
        if dry_run
        else publish.write_manifest(paths.LEGS_DIR, manifest_path)
    )
    print(f"manifest: {len(data['days'])} days, {data['start']} -> {data['end']}")

    payload, keys = _payload()
    local_keys = set(keys.values()) | {MANIFEST_KEY}
    total = sum(p.stat().st_size for p in payload)
    print(f"mirror: {len(payload) + 1} files, {total / 1e6:.1f} MB + manifest")

    r2 = publish.r2_from_env()
    if r2 is None:
        action = "inspected" if dry_run else "uploaded"
        print(
            f"R2 credentials not set — see .env.example. Nothing {action}.",
            file=sys.stderr,
        )
        return 1

    client = r2.client()
    existing = r2.existing_objects(client)
    print(f"bucket {r2.bucket}: {len(existing)} objects already present")

    pending: list[Path] = []
    current: list[Path] = []
    for p in payload:
        key = keys[p]
        remote = existing.get(key)
        if remote is not None and r2.object_matches(p, key, remote, client=client):
            current.append(p)
        else:
            pending.append(p)

    stale = managed_stale_keys(set(existing), local_keys)

    if dry_run:
        print(f"plan: {len(pending)} uploads, {len(current)} already current, + manifest last")
        _print_pending(pending, keys)
        if delete:
            print(f"  after manifest: {len(stale)} managed objects to delete")
            for key in stale[:5]:
                print(f"    {key}")
            if len(stale) > 5:
                print(f"    ... and {len(stale) - 5} more")
        elif stale:
            print(f"  leaving {len(stale)} obsolete managed objects (pass --delete to remove)")
        return 0

    sent = 0
    for p in pending:
        key = keys[p]
        r2.upload(p, key, client=client)
        sent += 1
        if sent % 25 == 0:
            print(f"  {sent} uploaded ({len(current)} already current)")

    # Commit point: everything the new manifest describes is now present. If the process dies
    # before this upload, clients continue using the old contract against a compatible superset
    # of static assets and facts.
    r2.upload(manifest_path, MANIFEST_KEY, client=client)

    deleted = 0
    if delete:
        # Deleting before the manifest would make old clients request files that now 404. Once
        # the new manifest is live these keys are unreachable garbage, so cleanup is safe.
        for key in stale:
            r2.delete(key, client=client)
            deleted += 1

    print(
        f"done: {sent} uploaded, {len(current)} already current, + {MANIFEST_KEY}, "
        f"{deleted} deleted"
    )
    if stale and not delete:
        print(f"note: {len(stale)} obsolete managed objects remain; rerun with --delete")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="compare local and remote mirrors without writing either one",
    )
    p.add_argument(
        "--delete",
        action="store_true",
        help="after manifest commit, delete remote managed objects absent locally",
    )
    args = p.parse_args()
    sys.exit(main(args.dry_run, args.delete))
