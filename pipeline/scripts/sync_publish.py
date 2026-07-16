"""Upload the local publish mirror to R2 — no recompute, just bytes.

data/publish is a byte-for-byte mirror of the bucket root, so this is a plain sync of
everything already staged there by the pipeline. Use it after a backfill that ran without
credentials, or to resume one that died partway.

Two rules the ordering encodes:

  - The manifest is regenerated from what is actually on disk, so it can never advertise a
    day that was never staged.
  - It uploads *last*, after every leg file — a half-finished sync leaves the site showing
    the old date range rather than pointing at days that aren't up yet.

Files already in the bucket at the same size are skipped, so re-running after an interrupted
sync costs one LIST instead of re-uploading gigabytes.

Usage:
    set -a; source .env; set +a
    uv run python scripts/sync_publish.py [--dry-run]
"""

import argparse
import sys

from berg_pipeline import paths, publish

MANIFEST_KEY = "manifest.json"


def main(dry_run: bool) -> int:
    manifest_path = paths.PUBLISH_ROOT / MANIFEST_KEY
    data = publish.write_manifest(paths.LEGS_DIR, manifest_path)
    print(f"manifest: {len(data['days'])} days, {data['start']} -> {data['end']}")

    files = sorted(p for p in paths.PUBLISH_ROOT.rglob("*") if p.is_file())
    keys = {p: p.relative_to(paths.PUBLISH_ROOT).as_posix() for p in files}
    payload = [p for p in files if keys[p] != MANIFEST_KEY]
    total = sum(p.stat().st_size for p in files)
    print(f"mirror: {len(files)} files, {total / 1e6:.1f} MB")

    if dry_run:
        for p in payload[:5]:
            print(f"  would upload {keys[p]}")
        print(f"  ... and {max(0, len(payload) - 5)} more, then {MANIFEST_KEY} last")
        return 0

    r2 = publish.r2_from_env()
    if r2 is None:
        print("R2 credentials not set — see .env.example. Nothing uploaded.", file=sys.stderr)
        return 1

    client = r2.client()
    existing = r2.existing_sizes(client)
    print(f"bucket {r2.bucket}: {len(existing)} objects already present")

    sent = skipped = 0
    for p in payload:
        key = keys[p]
        if existing.get(key) == p.stat().st_size:
            skipped += 1
            continue
        r2.upload(p, key, client=client)
        sent += 1
        if sent % 25 == 0:
            print(f"  {sent} uploaded ({skipped} skipped)")

    # Last, always: everything it describes is now up.
    r2.upload(manifest_path, MANIFEST_KEY, client=client)
    print(f"done: {sent} uploaded, {skipped} already current, + {MANIFEST_KEY}")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="show what would go up, touch nothing")
    sys.exit(main(p.parse_args().dry_run))
