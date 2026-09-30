"""The part of a European dataset build that does not depend on the source.

An adapter script stages service days and builds legs month by month; from there, checking
the minimum-leg floor, exporting the UTC day files and static dictionaries, and writing
quality.json are identical for every dataset.
"""

import json
import time
from datetime import date, timedelta

from berg_pipeline import ingest
from berg_pipeline.europe import catalog, stops
from berg_pipeline.europe.config import DatasetConfig


def months(first: date, last: date) -> list[tuple[date, date]]:
    """[first, last] cut at month boundaries, as (first, last) day pairs."""
    out, d = [], first
    while d <= last:
        nxt = (d.replace(day=1) + timedelta(days=32)).replace(day=1)
        out.append((d, min(last, nxt - timedelta(days=1))))
        d = nxt
    return out


def daterange(first: date, last: date) -> list[date]:
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def write_stations_json(cfg: DatasetConfig, passenger_only: bool = False) -> None:
    """static/stations.json from the dataset's dim_station parquet."""
    import duckdb

    where = "WHERE passenger" if passenger_only else ""
    served = duckdb.sql(
        f"SELECT bpuic, name, code, lon, lat FROM '{cfg.dim_station_parquet.as_posix()}' {where}"
    ).fetchall()
    static = cfg.publish_root / "static"
    static.mkdir(parents=True, exist_ok=True)
    (static / "stations.json").write_text(
        json.dumps(
            [{"id": i, "name": n, "code": c, "lon": lo, "lat": la} for i, n, c, lo, la in served],
            separators=(",", ":"),
            ensure_ascii=False,
        )
    )


def publish_days(
    con,
    cfg: DatasetConfig,
    first: date,
    last: date,
    quality: dict,
    skip_days: frozenset[date] = frozenset(),
    quiet_days: frozenset[date] = frozenset(),
) -> dict:
    """Legs already built in `con` → the dataset's publish mirror and quality.json.

    Every published day must clear the dataset's minimum-leg floor. A day that does not is a
    failed build, never a quiet day — this raises instead of exporting. The exceptions are a
    day the SOURCE records as mostly cancelled (a strike), which is published thin and listed
    in quality.json's source_cancelled_days, and a day in quiet_days: one the adapter shows
    the source's own timetable to be nearly empty (Christmas Day).

    skip_days are not exported at all: the source lost them, so the manifest lists them as
    missing coverage rather than as days without trains.
    """
    static = cfg.publish_root / "static"
    days = [d for d in daterange(first, last) if d not in skip_days]
    for day in skip_days:
        for sub in ("legs", "journeys"):
            (cfg.publish_root / sub / f"{day:%Y/%m/%d}.parquet").unlink(missing_ok=True)
    counts = stops.day_leg_counts(con, days)
    explained, thin = stops.thin_day_verdicts(
        con, {d: n for d, n in counts.items() if d not in quiet_days}, cfg.min_legs_per_day
    )
    if thin:
        raise RuntimeError(f"{len(thin)} day(s) under {cfg.min_legs_per_day} legs: {thin[:10]}")
    for d in explained:
        print(f"thin but source-attested: {d}")

    total_bytes = 0
    for day in days:
        legs = ingest.export_day(
            con,
            day,
            cfg.publish_root / "legs" / f"{day:%Y/%m/%d}.parquet",
            compression_level=cfg.compression_level,
        )
        journeys = ingest.export_journeys_day(
            con,
            day,
            cfg.publish_root / "journeys" / f"{day:%Y/%m/%d}.parquet",
            compression_level=cfg.compression_level,
        )
        total_bytes += legs["bytes"] + journeys["bytes"]
    print("train types:", ingest.export_train_types(con, static / "train_types.json"))
    print("route pairs:", ingest.export_route_pairs(con, static / "route_pairs.json"))

    skipped = ", ".join(str((d - date(1970, 1, 1)).days) for d in skip_days) or "-1"
    summary = con.execute(
        f"""SELECT count(*), count(*) FILTER (flags & 1 > 0),
                   count(DISTINCT (service_day, trip_id))
            FROM fct_legs WHERE t_dep >= ? AND t_dep < ?
              AND t_dep // 86400 NOT IN ({skipped})""",
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
        "source_quiet_days": [
            {"day": d.isoformat(), "legs": counts[d]} for d in sorted(quiet_days) if d in counts
        ],
        "skipped_days": [d.isoformat() for d in sorted(skip_days)],
        "max_day_legs": max(counts.values()),
        "quarantine": quarantine,
        "months": quality,
    }
    catalog.write_json(report, cfg.root / "quality.json")
    print(json.dumps({k: v for k, v in report.items() if k != "months"}, indent=1))
    return report
