"""Check every GTFS snapshot opens and carries a stops.txt — no full downloads.

GTFS_FP2018_2018-08-15.zip is truncated: 67 MB of local-header stream with no central
directory at all, so it fails to open (locally too — it is not a range-request artifact).
dim_station has to know which snapshots are usable before it trusts a weekly cadence.

Usage:
    uv run python scripts/gtfs_census.py --out ../docs/gtfs-census.json
"""

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from berg_pipeline.gtfs import Snapshot, latest_per_day, list_snapshots, stops_member
from berg_pipeline.remote_zip import open_remote_zip


def check(snap: Snapshot) -> dict:
    try:
        with open_remote_zip(snap.url) as zf:
            names = zf.namelist()
            info = zf.getinfo(stops_member(names))
        return {"ok": True, "members": len(names), "stops_bytes": info.file_size}
    except Exception as e:  # noqa: BLE001 — a broken snapshot is the finding
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--workers", type=int, default=8)
    a = p.parse_args()

    snaps = latest_per_day(list_snapshots())
    print(f"checking {len(snaps)} snapshots ({snaps[0].day} .. {snaps[-1].day})", flush=True)

    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        results = list(ex.map(check, snaps))

    out = {}
    for s, r in zip(snaps, results):
        out[s.day.isoformat()] = {"url": s.url, "fp_year": s.fp_year, **r}
        if not r["ok"]:
            print(f"  BROKEN {s.day}  {r['error']}  {s.url.split('/')[-1]}", flush=True)

    broken = [d for d, r in out.items() if not r["ok"]]
    print(f"\nusable: {len(snaps) - len(broken)}/{len(snaps)}   broken: {len(broken)}")
    for d in broken:
        print(f"  {d}")

    if a.out:
        a.out.write_text(json.dumps(out, indent=1, sort_keys=True))
        print(f"wrote {a.out}")
