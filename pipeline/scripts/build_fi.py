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
import json
import sys
import time
from datetime import date, timedelta

import duckdb

from berg_pipeline import ingest
from berg_pipeline.europe import catalog, fi, stops
from berg_pipeline.europe.config import FINLAND as CFG


def months(first: date, last: date) -> list[tuple[date, date]]:
    out, d = [], first
    while d <= last:
        nxt = (d.replace(day=1) + timedelta(days=32)).replace(day=1)
        out.append((d, min(last, nxt - timedelta(days=1))))
        d = nxt
    return out


def daterange(first: date, last: date) -> list[date]:
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def main(first: date, last: date, skip_stage: bool) -> int:
    t0 = time.monotonic()
    CFG.root.mkdir(parents=True, exist_ok=True)
    static = CFG.publish_root / "static"
    static.mkdir(parents=True, exist_ok=True)

    stations = fi.fetch_stations()
    print("stations:", fi.write_dim_station(stations, CFG.dim_station_parquet))
    served = duckdb.sql(
        f"SELECT bpuic, name, code, lon, lat FROM '{CFG.dim_station_parquet.as_posix()}'"
    ).fetchall()
    (static / "stations.json").write_text(
        json.dumps(
            [{"id": i, "name": n, "code": c, "lon": lo, "lat": la} for i, n, c, lo, la in served],
            separators=(",", ":"),
            ensure_ascii=False,
        )
    )

    con = duckdb.connect(str(CFG.duckdb_path))
    stops.create_tables(con)
    quality: dict[str, dict] = {}
    if not skip_stage:
        for m_first, m_last in months(first - timedelta(days=1), last + timedelta(days=1)):
            key = f"{m_first:%Y-%m}" if m_first.day == 1 else m_first.isoformat()
            staged = fi.stage_days(con, daterange(m_first, m_last))
            legs = stops.build_legs(con, CFG, m_first, m_last, CFG.dim_station_parquet)
            quality[key] = {"stage": staged, "legs": legs}
            print(f"{key}: {staged['stops_staged']:,} stops → {legs['legs_written']:,} legs "
                  f"{ {k: v for k, v in legs.items() if k not in ('ok', 'legs_written')} }",
                  flush=True)

    days = daterange(first, last)
    counts = stops.day_leg_counts(con, days)
    explained, thin = stops.thin_day_verdicts(con, counts, CFG.min_legs_per_day)
    if thin:
        raise RuntimeError(f"{len(thin)} day(s) under {CFG.min_legs_per_day} legs: {thin[:10]}")
    for d in explained:
        print(f"thin but source-attested: {d}")

    total_bytes = 0
    for day in days:
        legs = ingest.export_day(con, day, CFG.publish_root / "legs" / f"{day:%Y/%m/%d}.parquet")
        journeys = ingest.export_journeys_day(
            con, day, CFG.publish_root / "journeys" / f"{day:%Y/%m/%d}.parquet"
        )
        total_bytes += legs["bytes"] + journeys["bytes"]
    print("train types:", ingest.export_train_types(con, static / "train_types.json"))
    print("route pairs:", ingest.export_route_pairs(con, static / "route_pairs.json"))

    summary = con.execute(
        """SELECT count(*), count(*) FILTER (flags & 1 > 0), count(DISTINCT (service_day, trip_id))
           FROM fct_legs WHERE t_dep >= ? AND t_dep < ?""",
        [
            int((first - date(1970, 1, 1)).total_seconds()),
            int((last + timedelta(days=1) - date(1970, 1, 1)).total_seconds()),
        ],
    ).fetchone()
    quarantine = dict(
        con.execute("SELECT reason, count(*) FROM quarantine_legs GROUP BY 1").fetchall()
    )
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "days": len(days),
        "legs": summary[0],
        "legs_scheduled_fallback": summary[1],
        "journeys": summary[2],
        "fact_bytes": total_bytes,
        "bytes_per_leg_incl_sidecar": round(total_bytes / max(1, summary[0]), 2),
        "min_day_legs": min(counts.values()),
        "source_cancelled_days": explained,
        "max_day_legs": max(counts.values()),
        "quarantine": quarantine,
        "months": quality,
    }
    catalog.write_json(report, CFG.root / "quality.json")
    print(json.dumps({k: v for k, v in report.items() if k != "months"}, indent=1))
    print(f"done in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--first", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_start))
    p.add_argument("--last", type=date.fromisoformat, default=date.fromisoformat(CFG.coverage_end))
    p.add_argument("--skip-stage", action="store_true", help="export from the existing database")
    a = p.parse_args()
    sys.exit(main(a.first, a.last, a.skip_stage))
