"""Drive build_month.py across a range of months, then publish the manifest.

Usage:
    uv run python scripts/backfill.py 2023-01 2026-06 [--force]

Runs months in order — legs_parquet near a month boundary draws from two monthly fct_legs
partitions, and the manifest is only meaningful once earlier months are staged. --force
re-runs months that already look complete; without it, a month with >= 25 day files is
assumed done and skipped (some months legitimately have missing days, so this is a heuristic,
not an exact check against docs/archive-census.json).
"""

import argparse
import fcntl
import os
import sys
from pathlib import Path

# Raw CSVs are ~200-400 MB/month; keeping all 42 months on disk at once isn't necessary and
# a long backfill shouldn't fill the volume.
os.environ["BERG_DELETE_RAW"] = "1"

sys.path.insert(0, str(Path(__file__).parent))
import build_month  # noqa: E402

import dagster as dg  # noqa: E402

from berg_pipeline import paths  # noqa: E402
from berg_pipeline.assets import legs  # noqa: E402
from berg_pipeline.resources import default_duckdb  # noqa: E402

DONE_THRESHOLD = 25


def acquire_lock():
    """Refuse to run twice. Returns the held handle — the caller must keep it alive.

    Two concurrent backfills race on the same raw dir: one is mid-extraction while the other
    sees a partial month, and only DuckDB's own file lock stops the writes from overlapping.
    flock releases automatically when the process dies, so a killed run never leaves it stuck.
    """
    lock_path = paths.DATA_ROOT / "backfill.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(
            f"a backfill is already running (lock: {lock_path}).\n"
            "Check with: ps -eo pid,args | grep '[b]ackfill.py'"
        )
    fh.write(f"{os.getpid()}\n")
    fh.flush()
    return fh


def months_between(start: str, end: str):
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    while (y, m) <= (ey, em):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m > 12:
            m = 1
            y += 1


def month_looks_done(month: str) -> bool:
    day_dir = paths.LEGS_DIR / month[:4] / month[5:7]
    if not day_dir.exists():
        return False
    return len(list(day_dir.glob("*.parquet"))) >= DONE_THRESHOLD


def main(start: str, end: str, force: bool) -> int:
    _lock = acquire_lock()  # noqa: F841 — held for the process lifetime
    done, skipped, failed = [], [], []

    for month in months_between(start, end):
        if not force and month_looks_done(month):
            print(f"=== {month}: skipping (already staged) ===")
            skipped.append(month)
            continue
        try:
            build_month.main(month, skip_dim=True)
            done.append(month)
        except Exception as e:  # noqa: BLE001 — an unpublished current month must not kill the run
            print(f"=== {month}: FAILED — {e} ===")
            failed.append(month)

    print("\n=== manifest ===")
    result = dg.materialize(
        [legs.manifest, legs.legs_parquet.to_source_asset()],
        selection=[legs.manifest],
        resources={"duckdb": default_duckdb()},
    )
    assert result.success

    print(f"\n=== backfill {start} .. {end} ===")
    print(f"{'done':>10}: {len(done)} {done}")
    print(f"{'skipped':>10}: {len(skipped)} {skipped}")
    print(f"{'failed':>10}: {len(failed)} {failed}")
    if failed:
        print("\nRe-run the same range to retry only these — completed months are skipped.")
    # Non-zero on any failure: a month that dies still prints a tidy summary, and every silent
    # data loss found so far looked exactly like a successful run.
    return 1 if failed else 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("start", help="YYYY-MM")
    p.add_argument("end", help="YYYY-MM")
    p.add_argument("--force", action="store_true")
    a = p.parse_args()
    sys.exit(main(a.start, a.end, a.force))
