"""Upload the oversized, versioned DuckDB-Wasm modules to the public R2 bucket.

Cloudflare Pages rejects individual assets larger than 25 MiB. The small DuckDB worker
scripts remain in the Vite bundle, while these immutable WASM modules are served from R2.

Usage:
    set -a; source .env; set +a
    uv run python scripts/upload_web_runtime.py
"""

import json
import os
import sys
from pathlib import Path

from berg_pipeline import publish

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_CONFIG = REPO_ROOT / "web" / "duckdb-runtime.json"
DUCKDB_DIST = REPO_ROOT / "web" / "node_modules" / "@duckdb" / "duckdb-wasm" / "dist"
DUCKDB_PACKAGE = DUCKDB_DIST.parent / "package.json"


def runtime_assets() -> list[tuple[Path, str]]:
    config = json.loads(RUNTIME_CONFIG.read_text())
    version = config["version"]
    installed_version = json.loads(DUCKDB_PACKAGE.read_text())["version"]
    if installed_version != version:
        raise RuntimeError(
            f"duckdb-runtime.json requires {version}, but node_modules contains "
            f"{installed_version}; run npm ci in web/"
        )

    assets = []
    for filename in config["files"].values():
        path = DUCKDB_DIST / filename
        if not path.is_file():
            raise RuntimeError(f"missing DuckDB runtime asset: {path}")
        assets.append((path, f"static/duckdb-wasm/{version}/{filename}"))
    return assets


def main() -> int:
    try:
        assets = runtime_assets()
        r2 = publish.r2_from_env()
    except (OSError, KeyError, json.JSONDecodeError, RuntimeError) as error:
        print(f"preflight failed: {error}", file=sys.stderr)
        return 1
    if r2 is None:
        print("R2 credentials not set — see .env.example.", file=sys.stderr)
        return 1

    client = r2.client()
    existing = r2.existing_objects(client)
    uploaded = 0
    for path, key in assets:
        remote = existing.get(key)
        if remote is not None and r2.object_matches(path, key, remote, client=client):
            print(f"current: {key}")
            continue
        r2.upload(
            path,
            key,
            client=client,
            content_type="application/wasm",
            cache_control="public, max-age=31536000, immutable",
        )
        uploaded += 1
        print(f"uploaded: {key} ({path.stat().st_size / 1e6:.1f} MB)")

    public_url = os.environ.get("R2_PUBLIC_URL", "").rstrip("/")
    if public_url:
        for _, key in assets:
            print(f"  {public_url}/{key}")
    print(f"done: {uploaded} uploaded, {len(assets) - uploaded} already current")
    return 0


if __name__ == "__main__":
    sys.exit(main())
