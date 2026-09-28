"""Build the German dataset's publish mirror from the DB Timetables API archive.

    uv run python scripts/build_de.py --fetch     # raw months + StaDa, stage → legs → days
    (geometry job, see geometry/README.md)        # routes.bin
    uv run python scripts/sync_dataset.py de      # R2, manifest then catalog

Service days one either side of the coverage window are staged because UTC day files are cut
from Berlin service days. Months are fetched at the revision pinned in europe/de.py and
checked against their SHA-256; a StaDa station snapshot is kept for every month.
"""

import argparse
import json
import sys
import time
from datetime import date, timedelta

import duckdb

from berg_pipeline.europe import build, catalog, de, stops
from berg_pipeline.europe.config import GERMANY as CFG


def main(first: date, last: date, fetch: bool, skip_stage: bool, restage: bool) -> int:
    t0 = time.monotonic()
    CFG.root.mkdir(parents=True, exist_ok=True)
    spans = build.months(first - timedelta(days=1), last + timedelta(days=1))
    months = [f"{m_first:%Y-%m}" for m_first, _ in spans]
    if fetch:
        for month in months:
            print("fetched", de.fetch_month(month).name, flush=True)
            print("stations", de.fetch_stations(month).name, flush=True)
    snapshots = [de.stations_path(m) for m in months if de.stations_path(m).exists()]
    print("stations:", de.write_dim_station(snapshots, CFG.dim_station_parquet))
    build.write_stations_json(CFG)

    con = duckdb.connect(str(CFG.duckdb_path))
    # Three monthly files are read at once to stage one month. On an 8 GB WSL machine the
    # default (80% of RAM, every core) got the VM OOM-killed next to anything else.
    con.execute("SET memory_limit = '2GB'")
    con.execute("SET threads = 4")
    con.execute("SET preserve_insertion_order = false")
    stops.create_tables(con)
    # Each month commits on its own, so a restart resumes after the last finished month
    # instead of restaging ten of them. --restage ignores the record.
    progress = CFG.root / "stage_progress.json"
    quality: dict[str, dict] = (
        json.loads(progress.read_text()) if progress.exists() and not restage else {}
    )
    if not skip_stage:
        for m_first, m_last in spans:
            key = f"{m_first:%Y-%m}" if m_first.day == 1 else m_first.isoformat()
            if key in quality:
                continue
            staged = de.stage_days(con, build.daterange(m_first, m_last), CFG.dim_station_parquet)
            legs = stops.build_legs(con, CFG, m_first, m_last, CFG.dim_station_parquet)
            con.execute("CHECKPOINT")
            quality[key] = {"stage": staged, "legs": legs}
            catalog.write_json(quality, progress)
            print(f"{key}: {staged['stops_staged']:,} stops → {legs['legs_written']:,} legs "
                  f"{ {k: v for k, v in legs.items() if k not in ('ok', 'legs_written')} } "
                  f"road={staged['rides_road']} malformed={staged['rides_malformed']} "
                  f"added={staged['stops_additional_dropped']}", flush=True)

    report = build.publish_days(con, CFG, first, last, quality)
    gaps = de.gap_hours(first, last)
    report["source_gap_hours"] = gaps
    report["source_revision"] = f"huggingface.co/datasets/{de.REPO}@{de.REVISION}"
    catalog.write_json(report, CFG.root / "quality.json")
    print(f"source gap hours: {len(gaps)}")
    print(f"done in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--first", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_start))
    p.add_argument("--last", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_end))
    p.add_argument("--fetch", action="store_true", help="download missing raw months first")
    p.add_argument("--skip-stage", action="store_true", help="export from the existing database")
    p.add_argument("--restage", action="store_true", help="restage months already staged")
    a = p.parse_args()
    sys.exit(main(a.first, a.last, a.fetch, a.skip_stage, a.restage))
