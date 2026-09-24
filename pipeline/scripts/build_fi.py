"""Build the Finnish dataset's publish mirror from already-fetched Digitraffic days.

    uv run python scripts/fetch_fi.py 2022-12-31 2026-01-01   # raw, resumable
    uv run python scripts/build_fi.py                         # stage → legs → day files
    (geometry job, see geometry/README.md)                    # routes.bin
    uv run python scripts/sync_dataset.py fi                  # R2, manifest then catalog

Service days one either side of the coverage window are staged because UTC day files are cut
from Helsinki service days: the first UTC day's 00:00-02:00 belongs to the previous service
day, and the last UTC day's final two hours to the next one. Only UTC days inside the window
are published.

Every published day must clear the dataset's minimum-leg floor. A day that does not is a
failed build, never a quiet day — this script raises instead of writing a manifest. The one
exception is a day the SOURCE records as mostly cancelled (the 2023-2024 Finnish strikes):
those publish what did run and are listed in quality.json and the manifest's
source_cancelled_days, so the UI can say the railway stopped rather than show an unexplained
near-empty map.
"""

import argparse
import sys
import time
from datetime import date, timedelta

import duckdb

from berg_pipeline.europe import build, fi, stops
from berg_pipeline.europe.config import FINLAND as CFG


def main(first: date, last: date, skip_stage: bool) -> int:
    t0 = time.monotonic()
    CFG.root.mkdir(parents=True, exist_ok=True)

    stations = fi.fetch_stations()
    print("stations:", fi.write_dim_station(stations, CFG.dim_station_parquet))
    build.write_stations_json(CFG)

    con = duckdb.connect(str(CFG.duckdb_path))
    stops.create_tables(con)
    quality: dict[str, dict] = {}
    if not skip_stage:
        for m_first, m_last in build.months(first - timedelta(days=1), last + timedelta(days=1)):
            key = f"{m_first:%Y-%m}" if m_first.day == 1 else m_first.isoformat()
            staged = fi.stage_days(con, build.daterange(m_first, m_last))
            legs = stops.build_legs(con, CFG, m_first, m_last, CFG.dim_station_parquet)
            quality[key] = {"stage": staged, "legs": legs}
            print(f"{key}: {staged['stops_staged']:,} stops → {legs['legs_written']:,} legs "
                  f"{ {k: v for k, v in legs.items() if k not in ('ok', 'legs_written')} }",
                  flush=True)

    build.publish_days(con, CFG, first, last, quality)
    print(f"done in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--first", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_start))
    p.add_argument("--last", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_end))
    p.add_argument("--skip-stage", action="store_true", help="export from the existing database")
    a = p.parse_args()
    sys.exit(main(a.first, a.last, a.skip_stage))
