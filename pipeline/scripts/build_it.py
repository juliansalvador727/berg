"""Build the Italian dataset's publish mirror from TrainStats' archive.

    uv run python scripts/fetch_it.py             # raw days (resumable), Dropbox + Mega
    uv run python scripts/build_it.py             # extract, stations, stage → legs → days
    (geometry job, see geometry/README.md)        # routes.bin
    uv run python scripts/sync_dataset.py it      # R2, manifest then catalog

Service days one either side of the window are staged because UTC day files are cut from Rome
service days (a train is filed under the day it departs its origin). A day TrainStats did not
collect is not published: its file is missing or broken at the source, or its trains cover
under COLLECTED_SHARE of the trains Trenitalia's own summary says ran. The hours next to it
that its trains would have filled become source gap hours.
"""

import argparse
import csv
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta

import duckdb

from berg_pipeline.europe import build, catalog, it, it_fetch, stops
from berg_pipeline.europe.config import ITALY as CFG

EXTRACT_WORKERS = 4
# A day whose collected (non-cancelled) trains are under this share of the trains the source
# summary counts as having run is a collection outage. Measured: ordinary days 0.85-0.95;
# 2024-06-08 (database crash) 0.54, 2024-12-12/13 (IP block) 0.46 and 0.35.
COLLECTED_SHARE = 0.6
# A service day cancelling at least this share of its trains is a strike, published thin.
# Measured: strike days cancel 40-55%, ordinary days under 3%.
STRIKE_SHARE = 0.25


def source_days(first: date, last: date) -> tuple[list[date], dict[date, str]]:
    """(days to stage, {day: why it is not published}) for service days first..last."""
    ledger = it_fetch.load_ledger(CFG)
    staged, lost = [], {}
    for day in build.daterange(first, last):
        row = ledger.get(day.isoformat())
        if row is None:
            lost[day] = "absent from the archive"
        elif row["status"] != "ok":
            lost[day] = row["status"]
        else:
            staged.append(day)
    return staged, lost


def main(first: date, last: date, skip_stage: bool) -> int:
    t0 = time.monotonic()
    CFG.root.mkdir(parents=True, exist_ok=True)
    staged, lost = source_days(first - timedelta(days=1), last + timedelta(days=1))
    with ProcessPoolExecutor(EXTRACT_WORKERS) as pool:
        census = {date.fromisoformat(c["day"]): c for c in pool.map(it.extract_day, staged)}
    for day, c in census.items():
        ran = (c["trains"] - c["cancelled_trains"]) / max(1, c["circulated"] or 0)
        if c["circulated"] and ran < COLLECTED_SHARE:
            lost[day] = f"collection outage: {ran:.0%} of the trains that ran"
    # The migration day (2023-11-01) was exported by the previous database: no _id, trains
    # filed by arrival day and overlapping the day before. Not the current format.
    no_ids = duckdb.sql(f"""
        SELECT DISTINCT CAST(service_day AS DATE) FROM read_parquet(
            [{", ".join(f"'{it.extract_dir() / f'{d}.trains.parquet'}'" for d in staged)}])
        WHERE source_id IS NULL""").fetchall()
    for (day,) in no_ids:
        lost[day] = "previous-database format (no _id)"
    print("not collected:", {str(d): w for d, w in sorted(lost.items())})
    staged = [d for d in staged if d not in lost]

    it.fetch_stations()
    print("stations:", it.write_stations(staged))
    build.write_stations_json(CFG)

    con = duckdb.connect(str(CFG.duckdb_path))
    it.limit_memory(con)
    it.create_tables(con)
    fn_zero = it.fn_zero_stations(con, [d for d in staged if d < it.FN_FIX_DAY])
    print(f"FerrovieNord zero-delay stations before {it.FN_FIX_DAY}: {len(fn_zero)}")
    quality: dict[str, dict] = {}
    if not skip_stage:
        for m_first, m_last in build.months(first - timedelta(days=1), last + timedelta(days=1)):
            key = f"{m_first:%Y-%m}" if m_first.day == 1 else m_first.isoformat()
            days = [d for d in build.daterange(m_first, m_last) if d in set(staged)]
            staged_stats = it.stage_days(con, days, fn_zero) if days else {}
            legs = stops.build_legs(con, CFG, m_first, m_last, CFG.dim_station_parquet)
            quality[key] = {"stage": staged_stats, "legs": legs}
            print(
                f"{key}: {staged_stats.get('stops_staged', 0):,} stops → "
                f"{legs['legs_written']:,} legs "
                f"{ {k: v for k, v in legs.items() if k not in ('ok', 'legs_written')} } "
                f"trains={staged_stats.get('trains', 0):,} "
                f"cancelled={staged_stats.get('trains_cancelled', 0):,} "
                f"measured={staged_stats.get('stops_measured', 0) / max(1, staged_stats.get('stops_staged', 0)):.1%}",
                flush=True,
            )

    # A lost service day D takes UTC day D with it; its trains past midnight are missing from
    # the first local hours of D+1, and D's first trains from the last UTC hours of D-1.
    skip = {d for d in lost if first <= d <= last}
    gaps = set()
    for d in lost:
        for h in (22, 23):
            gaps.add(f"{d - timedelta(days=1)}T{h:02d}")
        for h in range(3):
            gaps.add(f"{d + timedelta(days=1)}T{h:02d}")
    gaps = {g for g in gaps if first.isoformat() <= g[:10] <= last.isoformat()}
    strikes = {
        d
        for (d,) in con.execute(
            "SELECT service_day FROM source_days WHERE service_day BETWEEN ? AND ? "
            "AND cancelled_trains >= ? * passenger_trains",
            [first, last, STRIKE_SHARE],
        ).fetchall()
    }
    print(f"strike days (published thin): {sorted(map(str, strikes))}")
    report = build.publish_days(
        con, CFG, first, last, quality, skip_days=frozenset(skip), quiet_days=frozenset(strikes)
    )
    report["source_gap_hours"] = sorted(g for g in gaps if date.fromisoformat(g[:10]) not in skip)
    report["not_collected"] = {str(d): w for d, w in sorted(lost.items())}
    report["fn_zero_delay_stations"] = fn_zero
    with open(CFG.root / "station_review.csv", newline="", encoding="utf-8") as fh:
        review = list(csv.DictReader(fh))
    report["stations"] = {
        s: sum(int(r["calls"]) for r in review if r["status"] == s)
        for s in sorted({r["status"] for r in review})
    }
    catalog.write_json(report, CFG.root / "quality.json")
    print(f"done in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        "--first", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_start)
    )
    p.add_argument("--last", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_end))
    p.add_argument("--skip-stage", action="store_true", help="export from the existing database")
    a = p.parse_args()
    sys.exit(main(a.first, a.last, a.skip_stage))
