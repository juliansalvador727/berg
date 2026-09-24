"""Build the Dutch dataset's publish mirror from the Rijden de Treinen train archive.

    uv run python scripts/build_nl.py --fetch     # raw months (resumable), stage → legs → days
    (geometry job, see geometry/README.md)        # routes.bin
    uv run python scripts/sync_dataset.py nl      # R2, manifest then catalog

Service days one either side of the coverage window are staged because UTC day files are cut
from Amsterdam service days: the first UTC day's early hours belong to the previous service
day, and the last UTC day's final hour to the next one. Only UTC days inside the window are
published. The archive has no monthly file before 2023, so the leading 2022-12 is split out
of the yearly file on first fetch.
"""

import argparse
import sys
import time
from datetime import date, timedelta

import duckdb

from berg_pipeline.europe import build, nl, stops
from berg_pipeline.europe.config import NETHERLANDS as CFG


def main(first: date, last: date, fetch: bool, skip_stage: bool) -> int:
    t0 = time.monotonic()
    CFG.root.mkdir(parents=True, exist_ok=True)
    spans = build.months(first - timedelta(days=1), last + timedelta(days=1))
    if fetch:
        nl.fetch_stations()
        for m_first, _ in spans:
            nl.fetch_month(f"{m_first:%Y-%m}")
    print("stations:", nl.write_dim_station(nl.stations_path(), CFG.dim_station_parquet))
    build.write_stations_json(CFG)

    con = duckdb.connect(str(CFG.duckdb_path))
    stops.create_tables(con)
    quality: dict[str, dict] = {}
    if not skip_stage:
        for m_first, m_last in spans:
            key = f"{m_first:%Y-%m}" if m_first.day == 1 else m_first.isoformat()
            staged = nl.stage_days(con, build.daterange(m_first, m_last), CFG.dim_station_parquet)
            legs = stops.build_legs(con, CFG, m_first, m_last, CFG.dim_station_parquet)
            quality[key] = {"stage": staged, "legs": legs}
            print(f"{key}: {staged['stops_staged']:,} stops → {legs['legs_written']:,} legs "
                  f"{ {k: v for k, v in legs.items() if k not in ('ok', 'legs_written')} } "
                  f"dup={staged['services_duplicate']} malformed={staged['services_malformed']}",
                  flush=True)

    build.publish_days(con, CFG, first, last, quality)
    print(f"done in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--first", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_start))
    p.add_argument("--last", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_end))
    p.add_argument("--fetch", action="store_true", help="download missing raw months first")
    p.add_argument("--skip-stage", action="store_true", help="export from the existing database")
    a = p.parse_args()
    sys.exit(main(a.first, a.last, a.fetch, a.skip_stage))
