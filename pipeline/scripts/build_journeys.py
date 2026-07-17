"""Generate the click-detail sidecar for every published day, from fct_legs.

A post-pass, not part of build_month: journeys/ is additive, so it can be produced long after
the legs it describes and never forces a re-export of them. Everything it needs is already in
fct_legs — this re-downloads nothing.

Run it AFTER a backfill, never during: DuckDB takes a single writer, and the pipeline holds
that lock for the whole run.

Usage:
    uv run python scripts/build_journeys.py                 # every day with a legs file
    uv run python scripts/build_journeys.py 2024-01 2024-12 # one month range
    uv run python scripts/build_journeys.py --dry-run
"""

import argparse
import sys
from datetime import date

from berg_pipeline import ingest, paths, publish
from berg_pipeline.resources import default_duckdb


def published_days(start: str | None, end: str | None) -> list[date]:
    """Mirror the legs mirror — a sidecar for a day with no legs file describes nothing."""
    days = []
    for f in sorted(paths.LEGS_DIR.glob("*/*/*.parquet")):
        d = date(int(f.parent.parent.name), int(f.parent.name), int(f.stem))
        if start and f"{d:%Y-%m}" < start:
            continue
        if end and f"{d:%Y-%m}" > end:
            continue
        days.append(d)
    return days


def main(start: str | None, end: str | None, dry_run: bool, force: bool) -> int:
    days = published_days(start, end)
    if not days:
        sys.exit("no published legs files in range — nothing to describe")

    todo = [d for d in days if force or not paths.journeys_parquet_path(d).exists()]
    print(f"{len(days)} published days, {len(todo)} without a sidecar")
    if dry_run:
        for d in todo[:5]:
            print(f"  would build journeys/{d:%Y/%m/%d}.parquet")
        print(f"  ... and {max(0, len(todo) - 5)} more")
        return 0
    if not todo:
        print("nothing to do")
        return 0

    r2 = publish.r2_from_env()
    client = r2.client() if r2 is not None else None
    print(f"upload: {'on' if r2 else 'OFF (staging locally only)'}")

    built = empty = 0
    total_bytes = total_rows = 0
    con = default_duckdb()
    with con.get_connection() as c:
        # route_id -> (from, to), once: the sidecar deliberately does not repeat it per leg.
        stats = ingest.export_route_pairs(c, paths.ROUTE_PAIRS_JSON)
        print(f"route_pairs.json: {stats['pairs']} pairs, {stats['bytes']:,} bytes")
        if r2 is not None:
            r2.upload(paths.ROUTE_PAIRS_JSON, "static/route_pairs.json", client=client)

        for i, day in enumerate(todo, 1):
            out = paths.journeys_parquet_path(day)
            st = ingest.export_journeys_day(c, day, out)
            if st["rows"] == 0:
                empty += 1
                continue
            built += 1
            total_rows += st["rows"]
            total_bytes += st["bytes"]
            if r2 is not None:
                key = f"journeys/{day.year:04d}/{day.month:02d}/{day.day:02d}.parquet"
                r2.upload(out, key, client=client)
            if i % 100 == 0:
                print(f"  {i}/{len(todo)}  ({total_bytes / 1e6:.0f} MB so far)")

    print(f"\nbuilt {built} sidecars ({empty} days had no legs)")
    if total_rows:
        print(f"{'total MB':>18}: {total_bytes / 1e6:.1f}")
        print(f"{'bytes/journey':>18}: {total_bytes / total_rows:.2f}")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("start", nargs="?", help="YYYY-MM (default: earliest published)")
    p.add_argument("end", nargs="?", help="YYYY-MM (default: latest published)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true", help="rebuild sidecars that already exist")
    a = p.parse_args()
    sys.exit(main(a.start, a.end, a.dry_run, a.force))
