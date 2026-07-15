"""Census every monthly ZIP's day members, without downloading any of them.

A ZIP's central directory carries each member's uncompressed size, and the archive serves
range requests — so the true coverage map of the whole archive costs one tail read per month.

This exists because the archive has holes and junk: 2019-07-01..16 are 20 KB stubs, whole days
are absent from some months, and three months carry __MACOSX resource forks that look like day
CSVs. Neither the backfill nor the frontend scrub bar can assume "every day in range has data".

Usage:
    uv run python scripts/archive_census.py --out ../docs/archive-census.json
"""

import argparse
import calendar
import json
from datetime import date, timedelta
from pathlib import Path

from berg_pipeline.archive import STUB_MAX_BYTES, day_members, url_for_month
from berg_pipeline.remote_zip import open_remote_zip

START = (2018, 1)


def months(start: tuple[int, int], end: tuple[int, int]):
    y, m = start
    while (y, m) <= end:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def census(end: tuple[int, int], v2: bool) -> dict:
    out = {}
    start = (2025, 7) if v2 else START
    for y, m in months(start, end):
        key = f"{y}-{m:02d}"
        try:
            with open_remote_zip(url_for_month(y, m, v2=v2)) as zf:
                days = {d: i.file_size for d, i in day_members(zf).items()}
                junk = sum(1 for i in zf.infolist() if i.filename.lower().endswith(".csv")) - len(
                    days
                )
        except Exception as e:  # noqa: BLE001 — census; a missing month is a finding
            print(f"{key}  ERROR {type(e).__name__}: {e}", flush=True)
            out[key] = {"error": f"{type(e).__name__}: {e}"}
            continue

        n_days = calendar.monthrange(y, m)[1]
        expected = {date(y, m, i + 1) for i in range(n_days)}
        absent = sorted(d.isoformat() for d in expected - days.keys())
        stubs = sorted(d.isoformat() for d, s in days.items() if s < STUB_MAX_BYTES)
        usable = sorted(d.isoformat() for d, s in days.items() if s >= STUB_MAX_BYTES)

        out[key] = {
            "usable_days": len(usable),
            "expected_days": n_days,
            "absent": absent,
            "stubs": stubs,
            "junk_members": junk,
            "total_bytes": sum(days.values()),
        }
        flags = []
        if absent:
            flags.append(f"{len(absent)} absent")
        if stubs:
            flags.append(f"{len(stubs)} stub")
        if junk:
            flags.append(f"{junk} junk")
        note = ("  ⚠ " + ", ".join(flags)) if flags else ""
        print(
            f"{key}  usable={len(usable):2d}/{n_days}  {sum(days.values()) / 1e9:5.1f} GB{note}",
            flush=True,
        )
    return out


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--v2", action="store_true", help="census the v2 archive (2025-07+)")
    a = p.parse_args()

    today = date.today()
    result = census((today.year, today.month), v2=a.v2)
    ok = {k: v for k, v in result.items() if "error" not in v}

    usable = sum(v["usable_days"] for v in ok.values())
    absent = sorted(d for v in ok.values() for d in v["absent"])
    stubs = sorted(d for v in ok.values() for d in v["stubs"])
    missing = sorted(absent + stubs)

    print(f"\nusable days:  {usable}")
    print(f"raw bytes:    {sum(v['total_bytes'] for v in ok.values()) / 1e12:.2f} TB")
    print(f"missing days: {len(missing)}  ({len(absent)} absent, {len(stubs)} stub)")
    for d in missing:
        print(f"  {d}")

    if missing:
        # Contiguous gaps matter more than the count: the frontend has to skip them, and a
        # multi-day gap is a visible hole in the scrub bar.
        runs, run = [], [missing[0]]
        for prev, cur in zip(missing, missing[1:]):
            if date.fromisoformat(cur) - date.fromisoformat(prev) == timedelta(days=1):
                run.append(cur)
            else:
                runs.append(run)
                run = [cur]
        runs.append(run)
        print("\ngaps:")
        for r in runs:
            print(f"  {r[0]} .. {r[-1]}  ({len(r)}d)" if len(r) > 1 else f"  {r[0]}")

    if a.out:
        a.out.write_text(json.dumps(result, indent=1, sort_keys=True))
        print(f"\nwrote {a.out}")
