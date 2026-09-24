"""The normalized stop-event table every European adapter produces, and the legs built from it.

This is europe.md's adapter contract made concrete. An adapter's only job is to fill
stg_stops for a range of service days: one row per COMMERCIAL stop of a passenger journey,
in travel order, with UTC epoch-second times and an explicit measured flag per side. From
there, candidate legs, quarantine, registries and the split rule are shared — the last three
with Switzerland itself (ingest.write_tagged_legs).

Unlike the Swiss Ist-Daten path, stop order comes from stop_seq, not from scheduled time:
every European source probed so far ships a real sequence, and ordering by it means a delayed
train can never reorder its own stops.
"""

from datetime import date
from pathlib import Path

from berg_pipeline import ingest
from berg_pipeline.constants import MAX_RAW_LEG_DURATION_S
from berg_pipeline.europe.config import DatasetConfig


def create_tables(con) -> None:
    ingest.create_fact_tables(con)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stg_stops (
            service_day     DATE    NOT NULL,  -- the source's own operating day, local time
            trip_id         VARCHAR NOT NULL,  -- unique within service_day
            operator        VARCHAR,
            train_number    VARCHAR,           -- the commercial number a passenger sees
            category        VARCHAR,           -- dataset-local train type code → type_id
            line            VARCHAR,
            station_id      BIGINT  NOT NULL,  -- dataset-local station id (station_namespace)
            stop_seq        INTEGER NOT NULL,
            sched_arr       BIGINT,            -- epoch seconds UTC
            sched_dep       BIGINT,
            act_arr         BIGINT,
            act_dep         BIGINT,
            arr_measured    BOOLEAN NOT NULL,
            dep_measured    BOOLEAN NOT NULL,
            source_revision BIGINT
        )""")
    create_source_days(con)


def create_source_days(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS source_days (
            service_day       DATE    NOT NULL PRIMARY KEY,
            passenger_trains  INTEGER NOT NULL,
            cancelled_trains  INTEGER NOT NULL
        )""")


# A published day under the floor passes only when the source cancelled at least this share of
# its passenger trains for the service day. Measured: the 2023-03 and 2023-12/2024-02 Finnish
# strike days cancel 96-99.9%; an ordinary day cancels under 3%.
SOURCE_CANCELLED_SHARE = 0.5


def thin_day_verdicts(con, counts: dict[date, int], floor: int) -> tuple[list, list]:
    """(explained, unexplained) days under the floor, judged against the source's own record.

    A UTC day is explained when the service day of the same date — which supplies all but its
    last two hours — was mostly cancelled at the source.
    """
    explained, unexplained = [], []
    for day, legs in sorted(counts.items()):
        if legs >= floor:
            continue
        row = con.execute(
            "SELECT passenger_trains, cancelled_trains FROM source_days WHERE service_day = ?",
            [day],
        ).fetchone()
        if row and row[0] and row[1] / row[0] >= SOURCE_CANCELLED_SHARE:
            explained.append({"day": day.isoformat(), "legs": legs,
                              "source_trains": row[0], "source_cancelled": row[1]})
        else:
            unexplained.append((day.isoformat(), legs))
    return explained, unexplained


def build_legs(
    con, cfg: DatasetConfig, first: date, last: date, dim_station_parquet: Path
) -> dict:
    """stg_stops for service days [first, last] → fct_legs, through the shared leg contract.

    Same rules as the Swiss builder, minus its two Swiss-only workarounds: times are already
    UTC epochs (no SOURCE_TZ conversion) and stop order is the source sequence. The one-clock
    rule is identical — a leg's duration is measured-minus-measured or
    scheduled-minus-scheduled, never a mix. See ingest.build_legs for why.
    """
    create_tables(con)
    lon_min, lat_min, lon_max, lat_max = cfg.bbox
    if cfg.countries:
        listed = ", ".join(f"'{c}'" for c in cfg.countries)
        foreign = f"f.country NOT IN ({listed}) OR t.country NOT IN ({listed})"
    else:
        foreign = "false"
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW _dim AS SELECT * FROM '{dim_station_parquet.as_posix()}'"
    )
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _cand AS
        WITH hop AS (
            SELECT service_day, trip_id, category, line,
                   station_id                   AS from_bpuic,
                   lead(station_id) OVER w      AS to_bpuic,
                   dep_measured AND lead(arr_measured) OVER w AS measured,
                   CASE WHEN dep_measured AND lead(arr_measured) OVER w
                        THEN act_dep ELSE sched_dep END AS dep_used,
                   CASE WHEN dep_measured AND lead(arr_measured) OVER w
                        THEN lead(act_arr) OVER w ELSE lead(sched_arr) OVER w END AS arr_next,
                   CASE WHEN dep_measured AND sched_dep IS NOT NULL
                        THEN act_dep - sched_dep END AS delay_s,
                   -- A hop runs only if its origin has a departure and its destination an
                   -- arrival. Adapters null the side of a stop the source cancelled, so a
                   -- train cut short mid-route and resumed later never gets a bridging leg.
                   (sched_dep IS NOT NULL OR act_dep IS NOT NULL)
                     AND (lead(sched_arr) OVER w IS NOT NULL
                          OR lead(act_arr) OVER w IS NOT NULL) AS runs
            FROM stg_stops
            WHERE service_day BETWEEN DATE '{first}' AND DATE '{last}'
            WINDOW w AS (PARTITION BY service_day, trip_id ORDER BY stop_seq)
        )
        SELECT service_day, trip_id, category, line, from_bpuic, to_bpuic, measured, delay_s,
               dep_used AS t_dep, arr_next - dep_used AS dur, runs
        FROM hop
        WHERE to_bpuic IS NOT NULL""")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _tagged AS
        SELECT c.* EXCLUDE (runs),
               CASE
                 WHEN NOT c.runs                             THEN 'not_run'
                 WHEN c.t_dep IS NULL OR c.dur IS NULL       THEN 'missing_time'
                 WHEN c.dur < 0                              THEN 'negative_duration'
                 WHEN c.dur = 0                              THEN 'zero_duration'
                 WHEN c.dur > {MAX_RAW_LEG_DURATION_S}       THEN 'absurd_duration'
                 WHEN f.bpuic IS NULL OR t.bpuic IS NULL     THEN 'unmatched_station'
                 WHEN f.lon NOT BETWEEN {lon_min} AND {lon_max}
                   OR f.lat NOT BETWEEN {lat_min} AND {lat_max}
                   OR t.lon NOT BETWEEN {lon_min} AND {lon_max}
                   OR t.lat NOT BETWEEN {lat_min} AND {lat_max} THEN 'outside_bbox'
                 WHEN {foreign}                              THEN 'outside_country'
                 ELSE 'ok'
               END AS verdict
        FROM _cand c
        LEFT JOIN _dim f ON f.bpuic = c.from_bpuic
                        AND c.service_day BETWEEN f.valid_from AND f.valid_to
        LEFT JOIN _dim t ON t.bpuic = c.to_bpuic
                        AND c.service_day BETWEEN t.valid_from AND t.valid_to""")

    ingest.write_tagged_legs(con, first, last)

    stats = dict(
        con.execute(f"""
        SELECT verdict, count(*) FROM _tagged GROUP BY verdict
        UNION ALL SELECT 'legs_written', count(*)
        FROM fct_legs WHERE service_day BETWEEN DATE '{first}' AND DATE '{last}'""").fetchall()
    )
    stats.setdefault("ok", 0)
    return stats


def day_leg_counts(con, days: list[date]) -> dict[date, int]:
    """Published legs per UTC departure day — the unit export_day writes and ships."""
    out = {}
    for day in days:
        lo = int((day - date(1970, 1, 1)).total_seconds())
        out[day] = con.execute(
            "SELECT count(*) FROM fct_legs WHERE t_dep >= ? AND t_dep < ?", [lo, lo + 86400]
        ).fetchone()[0]
    return out
