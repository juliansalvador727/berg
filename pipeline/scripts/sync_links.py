"""Upload the cross-border layer to R2 under links/, then list it in the catalog.

Same order as sync_dataset.py: crosswalk, routes and day files first, then links/manifest.json,
then catalog.json. Clients reach the layer only through the catalog, so an interrupted sync
leaves every client on the previous layer or none.

Usage:
    set -a; source .env; set +a
    uv run python scripts/sync_links.py [--dry-run]
"""

import argparse
import json
import sys

from berg_pipeline import publish
from berg_pipeline.europe import catalog, links
from berg_pipeline.europe.config import R2_BUDGET_BYTES


def main(dry_run: bool) -> int:
    root = links.LINKS_ROOT / "publish"
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest["bridge_routes"] and not (root / "static" / "routes.bin").exists():
        print("preflight: bridges exist but static/routes.bin is missing", file=sys.stderr)
        return 1
    files = [p for p in root.rglob("*") if p.is_file() and p.name != "manifest.json"
             and not p.name.endswith(".report.json")]
    items = sorted(((p, links.LINKS_KEY_PREFIX + p.relative_to(root).as_posix()) for p in files),
                   key=lambda it: (not it[1].startswith("links/days/"), it[1]))
    local_bytes = sum(p.stat().st_size for p, _ in items) + manifest_path.stat().st_size
    print(f"payload: {len(items)} files + manifest, {local_bytes / 1e6:.1f} MB")

    r2 = publish.r2_from_env()
    if r2 is None:
        print("R2 credentials not set — see .env.example. Nothing uploaded.", file=sys.stderr)
        return 1
    client = r2.client()
    remote = r2.existing_objects(client)
    remote_total = sum(o["size"] for o in remote.values())
    remote_links = sum(o["size"] for k, o in remote.items() if k.startswith(links.LINKS_KEY_PREFIX))
    projected = remote_total - remote_links + local_bytes
    print(f"inventory: bucket {remote_total / 1e9:.3f} GB now, {projected / 1e9:.3f} GB projected")
    if projected > R2_BUDGET_BYTES:
        print(f"budget gate: {projected / 1e9:.3f} GB exceeds {R2_BUDGET_BYTES / 1e9:.1f} GB",
              file=sys.stderr)
        return 1
    pending = [(p, k) for p, k in items
               if k not in remote or not r2.object_matches(p, k, remote[k], client=client)]
    stale = [k for k in remote if k.startswith(links.LINKS_KEY_PREFIX)
             and k not in {key for _, key in items} and k != links.LINKS_KEY_PREFIX + "manifest.json"]
    print(f"plan: {len(pending)} uploads, {len(items) - len(pending)} current, "
          f"{len(stale)} stale day files to delete after the manifest")
    if dry_run:
        return 0

    for n, (p, key) in enumerate(pending, 1):
        # Unlike a dataset's immutable day files, the whole layer is rebuilt whenever a dataset
        # changes, so these may change under the same key: an hour, not forever.
        r2.upload(p, key, client=client, cache_control="public, max-age=3600",
                  content_type="application/json" if p.suffix == ".json" else None)
        if n % 200 == 0:
            print(f"  {n}/{len(pending)} uploaded", flush=True)
    r2.upload(manifest_path, links.LINKS_KEY_PREFIX + "manifest.json", client=client,
              content_type="application/json", cache_control="public, max-age=300")
    live = r2.get_json(catalog.CATALOG_KEY, client=client)
    cat = catalog.build_catalog({}, existing=live, links={"path": links.LINKS_KEY_PREFIX.rstrip("/")})
    cat_path = links.LINKS_ROOT / catalog.CATALOG_KEY
    catalog.write_json(cat, cat_path)
    r2.upload(cat_path, catalog.CATALOG_KEY, client=client, content_type="application/json",
              cache_control="public, max-age=300")
    for key in stale:
        r2.delete(key, client=client)
    print(f"done: {len(pending)} uploaded + manifest + catalog, {len(stale)} stale deleted")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    sys.exit(main(p.parse_args().dry_run))
