"""Upload one European dataset's publish mirror to R2 under datasets/<id>/, then the catalog.

The Swiss root layout is never touched: every key written here lives under the dataset's
prefix, except catalog.json, which is written last and only lists datasets whose manifest is
already live. The order mirrors sync_publish.py — static and facts, then the dataset's
manifest, then the catalog — so an interrupted sync leaves every client on a coherent
contract.

Before any byte is written, a budget gate refuses the upload if the dataset would exceed its
own allocation or the bucket's projected size would exceed europe.md's 9.0 GB ceiling.

Usage:
    set -a; source .env; set +a
    uv run python scripts/sync_dataset.py fi [--dry-run]
"""

import argparse
import json
import sys
from pathlib import Path

from berg_pipeline import publish, routesbin
from berg_pipeline.europe import catalog
from berg_pipeline.europe.config import DATASETS, R2_BUDGET_BYTES, DatasetConfig

REQUIRED_STATIC = ("route_pairs.json", "routes.bin", "stations.json", "train_types.json")


def validate_mirror(cfg: DatasetConfig) -> dict:
    root = cfg.publish_root
    static = root / "static"
    missing = [n for n in REQUIRED_STATIC if not (static / n).is_file()]
    if missing:
        raise RuntimeError(f"{cfg.dataset_id}: required static artifacts missing: {missing}")
    legs = {p.relative_to(root / "legs").as_posix() for p in (root / "legs").glob("*/*/*.parquet")}
    journeys = {
        p.relative_to(root / "journeys").as_posix()
        for p in (root / "journeys").glob("*/*/*.parquet")
    }
    if legs != journeys:
        raise RuntimeError(
            f"{cfg.dataset_id}: legs/journeys mismatch: {sorted(legs ^ journeys)[:3]}"
        )
    pairs = {int(r) for r in json.loads((static / "route_pairs.json").read_text())}
    geometry = set(routesbin.read_header(static / "routes.bin").route_ids)
    if pairs - geometry:
        raise RuntimeError(
            f"{cfg.dataset_id}: routes.bin is stale, {len(pairs - geometry)} route ids have no "
            f"geometry (first: {sorted(pairs - geometry)[:5]}). Run the geometry job."
        )
    return {"days": len(legs), "routes": len(pairs)}


def payload(cfg: DatasetConfig) -> list[tuple[Path, str]]:
    """(local path, key) in deployment order: static, journeys, legs. Manifest excluded."""
    order = {"static": 0, "journeys": 1, "legs": 2}
    files = [
        p for p in cfg.publish_root.rglob("*")
        if p.is_file() and p.name != "manifest.json" and not p.name.startswith(".")
    ]
    items = [(p, cfg.key_prefix + p.relative_to(cfg.publish_root).as_posix()) for p in files]
    return sorted(items, key=lambda it: (order.get(it[1].split("/")[2], 9), it[1]))


def budget(cfg: DatasetConfig, local_bytes: int, remote: dict[str, dict]) -> dict:
    """Projected bucket size once this dataset's prefix is replaced by the local mirror."""
    remote_total = sum(o["size"] for o in remote.values())
    remote_dataset = sum(o["size"] for k, o in remote.items() if k.startswith(cfg.key_prefix))
    projected = remote_total - remote_dataset + local_bytes
    report = {
        "dataset_bytes": local_bytes,
        "dataset_cap_bytes": cfg.storage_cap_bytes,
        "bucket_bytes_now": remote_total,
        "bucket_bytes_projected": projected,
        "bucket_budget_bytes": R2_BUDGET_BYTES,
        "objects_projected": len(remote) - sum(1 for k in remote if k.startswith(cfg.key_prefix)),
    }
    if local_bytes > cfg.storage_cap_bytes:
        raise RuntimeError(
            f"{cfg.dataset_id}: {local_bytes / 1e9:.3f} GB exceeds its "
            f"{cfg.storage_cap_bytes / 1e9:.3f} GB allocation"
        )
    if projected > R2_BUDGET_BYTES:
        raise RuntimeError(
            f"projected bucket {projected / 1e9:.3f} GB exceeds the "
            f"{R2_BUDGET_BYTES / 1e9:.1f} GB budget"
        )
    return report


def main(dataset_id: str, dry_run: bool) -> int:
    cfg = DATASETS[dataset_id]
    try:
        checked = validate_mirror(cfg)
    except (OSError, ValueError, RuntimeError) as error:
        print(f"preflight failed: {error}", file=sys.stderr)
        return 1
    print(f"preflight: {checked['days']} days, {checked['routes']} routes with geometry")

    manifest = catalog.build_dataset_manifest(cfg)
    manifest_path = cfg.publish_root / "manifest.json"
    if not dry_run:
        catalog.write_json(manifest, manifest_path)
    print(
        f"manifest: {len(manifest['days'])} days, {manifest['start']} -> {manifest['end']}, "
        f"{len(manifest['missing_days'])} missing"
    )

    items = payload(cfg)
    local_bytes = sum(p.stat().st_size for p, _ in items) + len(json.dumps(manifest))

    r2 = publish.r2_from_env()
    if r2 is None:
        print("R2 credentials not set — see .env.example. Nothing uploaded.", file=sys.stderr)
        return 1
    client = r2.client()
    remote = r2.existing_objects(client)
    try:
        inventory = budget(cfg, local_bytes, remote)
    except RuntimeError as error:
        print(f"budget gate: {error}", file=sys.stderr)
        return 1
    inventory["objects_projected"] += len(items) + 1
    print("inventory:", json.dumps(inventory))

    pending = [
        (p, k) for p, k in items
        if k not in remote or not r2.object_matches(p, k, remote[k], client=client)
    ]
    print(f"plan: {len(pending)} uploads, {len(items) - len(pending)} current, + manifest, "
          f"+ {catalog.CATALOG_KEY}")
    if dry_run:
        return 0

    for n, (p, key) in enumerate(pending, 1):
        r2.upload(p, key, client=client)
        if n % 100 == 0:
            print(f"  {n}/{len(pending)} uploaded", flush=True)
    r2.upload(manifest_path, cfg.key_prefix + "manifest.json", client=client,
              content_type="application/json")

    # Last commit point: the dataset becomes discoverable only now that its manifest is live.
    live = r2.get_json(catalog.CATALOG_KEY, client=client)
    cat = catalog.build_catalog({dataset_id: manifest}, existing=live)
    cat_path = cfg.root / catalog.CATALOG_KEY
    catalog.write_json(cat, cat_path)
    r2.upload(cat_path, catalog.CATALOG_KEY, client=client, content_type="application/json",
              cache_control="public, max-age=300")
    catalog.write_json(inventory, cfg.root / "inventory.json")
    print(f"done: {len(pending)} uploaded + manifest + catalog ({sorted(cat['datasets'])})")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("dataset", choices=sorted(DATASETS))
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    sys.exit(main(a.dataset, a.dry_run))
