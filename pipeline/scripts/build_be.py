"""Build the Belgian dataset's publish mirror from Infrabel's monthly raw punctuality files.

    uv run python scripts/build_be.py --fetch     # raw months (resumable), stage → legs → days
    (geometry job, see geometry/README.md)        # routes.bin
    uv run python scripts/sync_dataset.py be      # R2, manifest then catalog

Service days one either side of the coverage window are staged because UTC day files are cut
from Brussels service days. The source server gives each connection a few hundred KB/s, so
--fetch downloads months in parallel and resumes partial files.
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import duckdb

from berg_pipeline.europe import be, build, stops
from berg_pipeline.europe.config import BELGIUM as CFG


def fetch(spans: list[tuple[date, date]], workers: int) -> None:
    months = [f"{m_first:%Y-%m}" for m_first, _ in spans]
    with ThreadPoolExecutor(workers) as pool:
        for path in pool.map(be.fetch_month, months):
            print(f"fetched {path.name}", flush=True)


def main(first: date, last: date, fetch_workers: int | None, skip_stage: bool) -> int:
    t0 = time.monotonic()
    CFG.root.mkdir(parents=True, exist_ok=True)
    spans = build.months(first - timedelta(days=1), last + timedelta(days=1))
    if fetch_workers:
        be.fetch_stations()
        fetch(spans, fetch_workers)
    print("stations:", be.write_dim_station(be.stations_path(), CFG.dim_station_parquet))

    con = duckdb.connect(str(CFG.duckdb_path))
    stops.create_tables(con)
    quality: dict[str, dict] = {}
    if not skip_stage:
        for m_first, m_last in spans:
            key = f"{m_first:%Y-%m}" if m_first.day == 1 else m_first.isoformat()
            staged = be.stage_days(con, build.daterange(m_first, m_last), CFG.dim_station_parquet)
            legs = stops.build_legs(con, CFG, m_first, m_last, CFG.dim_station_parquet)
            quality[key] = {"stage": staged, "legs": legs}
            print(f"{key}: {staged['stops_staged']:,} stops → {legs['legs_written']:,} legs "
                  f"{ {k: v for k, v in legs.items() if k not in ('ok', 'legs_written')} } "
                  f"malformed={staged['trains_malformed']}", flush=True)

    # The ptcar list also holds junctions, sidings and workshops no passenger can use.
    build.write_stations_json(CFG, passenger_only=True)
    build.publish_days(con, CFG, first, last, quality)
    print(f"done in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--first", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_start))
    p.add_argument("--last", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_end))
    p.add_argument("--fetch", type=int, nargs="?", const=16, default=None, metavar="WORKERS",
                   help="download missing raw months first, WORKERS in parallel (default 16)")
    p.add_argument("--skip-stage", action="store_true", help="export from the existing database")
    a = p.parse_args()
    sys.exit(main(a.first, a.last, a.fetch, a.skip_stage))
