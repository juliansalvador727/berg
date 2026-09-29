"""Build the British dataset's publish mirror from the archived Darwin Push Port.

    uv run python scripts/build_gb.py --fetch     # raw hours (resumable), extract, stage → days
    (geometry job, see geometry/README.md)        # routes.bin
    uv run python scripts/sync_dataset.py gb      # R2, manifest then catalog

Service days one either side of the window are staged because UTC day files are cut from
London service days, and each service day is staged from the extracts of the day before (the
timetable for day D is partly loaded on D-1) and after (trains past midnight). --fetch
downloads oldest first; a finished hour file is never downloaded again.
"""

import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date, timedelta

import duckdb
import httpx

from berg_pipeline.europe import build, catalog, gb, stops
from berg_pipeline.europe.config import GREAT_BRITAIN as CFG

FETCH_WORKERS = 4  # days downloaded at once from a one-person archive
EXTRACT_WORKERS = 5
# A service day with under this share of the median day's timetabled trains is quiet.
QUIET_SHARE = 0.25


def fetch(days: list[date]) -> None:
    with httpx.Client(headers={"User-Agent": gb.USER_AGENT}, timeout=300) as client:
        gb.fetch_stations(client)
        with ThreadPoolExecutor(FETCH_WORKERS) as pool:
            for day, got in zip(days, pool.map(lambda d: gb.fetch_day(client, d), days)):
                if got:
                    print(f"fetched {day}: {got / 1e6:.0f} MB", flush=True)


def main(first: date, last: date, do_fetch: bool, skip_stage: bool) -> int:
    t0 = time.monotonic()
    CFG.root.mkdir(parents=True, exist_ok=True)
    # Service days first-1..last+1, the day before them (early timetable loads) and last+1
    # itself; the day after that only adds the last staged day's past-midnight actuals.
    archive = build.daterange(first - timedelta(days=2), last + timedelta(days=1))
    archive = [d for d in archive if d >= date.fromisoformat("2025-09-07")]
    if do_fetch:
        fetch(archive)
    with ProcessPoolExecutor(EXTRACT_WORKERS) as pool:
        for r in pool.map(gb.extract_day, archive):
            if not r.get("cached"):
                print("extracted", r, flush=True)

    print(
        "stations:", gb.write_dim_station(CFG.raw_dir / "naptan_stops.csv", CFG.dim_station_parquet)
    )
    build.write_stations_json(CFG)

    con = duckdb.connect(str(CFG.duckdb_path))
    stops.create_tables(con)
    quality: dict[str, dict] = {}
    if not skip_stage:
        for m_first, m_last in build.months(first - timedelta(days=1), last + timedelta(days=1)):
            key = f"{m_first:%Y-%m}" if m_first.day == 1 else m_first.isoformat()
            staged: dict[str, int] = {}
            for day in build.daterange(m_first, m_last):
                for k, v in gb.stage_days(con, [day]).items():
                    staged[k] = staged.get(k, 0) + v
            legs = stops.build_legs(con, CFG, m_first, m_last, CFG.dim_station_parquet)
            quality[key] = {"stage": staged, "legs": legs}
            print(
                f"{key}: {staged['stops_staged']:,} stops → {legs['legs_written']:,} legs "
                f"{ {k: v for k, v in legs.items() if k not in ('ok', 'legs_written')} } "
                f"services={staged['passenger_services']:,} "
                f"cancelled={staged['cancelled_services']:,} "
                f"part_cancelled={staged['part_cancelled_services']:,}",
                flush=True,
            )

    # A service day without its timetable load is not published. Its trains past midnight
    # are missing from the next UTC day's first hours too, which become source gaps.
    lost = gb.lost_timetable_days(first, last)
    gaps = set(gb.gap_hours(first, last))
    for day in lost:
        nxt = day + timedelta(days=1)
        gaps |= {f"{nxt}T{h:02d}" for h in range(3)}
    # A nearly empty timetable with its load intact is the railway's own quiet day
    # (Christmas Day), published thin rather than failing the floor.
    median = con.execute(
        "SELECT median(passenger_trains) FROM source_days WHERE service_day BETWEEN ? AND ?",
        [first, last],
    ).fetchone()[0]
    quiet = {
        d
        for (d,) in con.execute(
            "SELECT service_day FROM source_days WHERE service_day BETWEEN ? AND ? "
            "AND passenger_trains < ?",
            [first, last, QUIET_SHARE * median],
        ).fetchall()
    } - set(lost)
    print(f"lost timetable days: {lost}; quiet days: {sorted(quiet)}")
    report = build.publish_days(
        con, CFG, first, last, quality, skip_days=frozenset(lost), quiet_days=frozenset(quiet)
    )
    report["source_gap_hours"] = sorted(g for g in gaps if g[:10] not in {str(d) for d in lost})
    catalog.write_json(report, CFG.root / "quality.json")
    print(f"source gap hours: {report['source_gap_hours']}")
    print(f"done in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        "--first", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_start)
    )
    p.add_argument("--last", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_end))
    p.add_argument("--fetch", action="store_true", help="download missing raw hours first")
    p.add_argument("--skip-stage", action="store_true", help="export from the existing database")
    a = p.parse_args()
    sys.exit(main(a.first, a.last, a.fetch, a.skip_stage))
