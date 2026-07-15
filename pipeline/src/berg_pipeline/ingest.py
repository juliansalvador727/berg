"""The ingest SQL: daily Ist-Daten CSVs → staged stop events → legs → per-day wire Parquet.

This is the M0 spike SQL grown into the real contract (see docs/data-notes.md for why each
rule exists):

  - Columns are selected BY NAME from COLUMNS_ALL_ERAS — one query spans 2018 → now, because
    the archive's only schema change (SLOID, 2025-11) is a column we don't use.
  - Measured means *_PROGNOSE_STATUS IN MEASURED_STATUSES. The set match is era-free and
    survives 2018-05-06, the one day that mixes GESCHAETZT and REAL.
  - Stops are ordered by SCHEDULED time; there is no sequence column and actual times go
    non-monotonic under delay.
  - Legs that fail the contract are quarantined with a reason, never silently dropped.
    Negative durations are genuine source error (~0.44%/day, mostly CH→CH).
  - Legs longer than MAX_LEG_DURATION_S are split into synthetic sub-legs so the frontend's
    scrub query stays bounded: everything in flight at T departed in [T-3600, T].

All functions take an open DuckDB connection and are idempotent per month: re-running a month
deletes its rows first. route_id and type_id registries only ever append, so identifiers are
stable across re-runs.
"""

import calendar
from datetime import date, timedelta
from pathlib import Path

from berg_pipeline.constants import (
    CH_BBOX,
    FLAG_SCHEDULED_FALLBACK,
    FLAG_SYNTHETIC_SPLIT,
    MAX_LEG_DURATION_S,
    MEASURED_STATUSES,
    SOURCE_TZ,
)

# Scheduled times are 'DD.MM.YYYY HH:MM', actual 'DD.MM.YYYY HH:MM:SS' — cleanly split by
# column in every probed era, but try_strptime with both formats costs nothing and the
# invariant is not contractual.
TS_FORMATS = "['%d.%m.%Y %H:%M:%S','%d.%m.%Y %H:%M']"

_MEASURED_SQL = "(" + ", ".join(f"'{s}'" for s in MEASURED_STATUSES) + ")"


def month_bounds(month: str) -> tuple[date, date]:
    """'YYYY-MM' → (first day, last day)."""
    y, m = int(month[:4]), int(month[5:7])
    return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])


def create_tables(con) -> None:
    """Persistent pipeline tables. Registries append-only; facts delete-and-insert by month."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS stg_istdaten (
            service_day  DATE      NOT NULL,
            trip_id      VARCHAR   NOT NULL,
            operator     VARCHAR,
            category     VARCHAR,
            bpuic        BIGINT,
            sched_arr    TIMESTAMP,
            sched_dep    TIMESTAMP,
            act_arr      TIMESTAMP,
            act_dep      TIMESTAMP,
            arr_measured BOOLEAN   NOT NULL,
            dep_measured BOOLEAN   NOT NULL
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS fct_legs (
            service_day DATE     NOT NULL,
            trip_id     VARCHAR  NOT NULL,
            route_id    INTEGER  NOT NULL,
            from_bpuic  BIGINT   NOT NULL,
            to_bpuic    BIGINT   NOT NULL,
            t_dep       BIGINT   NOT NULL,   -- epoch seconds UTC
            dur         INTEGER  NOT NULL,   -- 1 .. MAX_LEG_DURATION_S after the split rule
            type_id     SMALLINT NOT NULL,
            delay       SMALLINT NOT NULL,   -- departure delay, SECONDS, clamped to int16
            flags       TINYINT  NOT NULL
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_legs (
            service_day DATE    NOT NULL,
            trip_id     VARCHAR NOT NULL,
            from_bpuic  BIGINT,
            to_bpuic    BIGINT,
            t_dep       BIGINT,
            dur         BIGINT,
            reason      VARCHAR NOT NULL
        )""")
    # (from, to) → route_id. The geometry job (M2) consumes this and produces routes.bin;
    # ids are first-seen order and never change once assigned.
    con.execute("""
        CREATE TABLE IF NOT EXISTS station_pairs (
            from_bpuic BIGINT  NOT NULL,
            to_bpuic   BIGINT  NOT NULL,
            route_id   INTEGER NOT NULL,
            PRIMARY KEY (from_bpuic, to_bpuic)
        )""")
    # category text → uint8 for the wire. 255 is reserved for "unknown".
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_train_type (
            category VARCHAR  NOT NULL PRIMARY KEY,
            type_id  SMALLINT NOT NULL
        )""")


def stage_month(con, month: str, csv_files: list[Path]) -> dict:
    """Daily CSVs → normalized train stop events for one month.

    all_varchar because autodetect silently typed BETRIEBSTAG as DATE on a real file;
    union_by_name because SLOID appears in 2025-11 and files within a month could straddle it.
    Cancelled stops are dropped (counted here) — decided at M1; revisit if ghost trains ever
    become a feature.
    """
    create_tables(con)
    first, last = month_bounds(month)
    files_sql = "[" + ", ".join(f"'{p.as_posix()}'" for p in csv_files) + "]"

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _raw AS
        SELECT BETRIEBSTAG, FAHRT_BEZEICHNER, BETREIBER_ABK, VERKEHRSMITTEL_TEXT,
               PRODUKT_ID, FAELLT_AUS_TF, BPUIC,
               ANKUNFTSZEIT, AN_PROGNOSE, AN_PROGNOSE_STATUS,
               ABFAHRTSZEIT, AB_PROGNOSE, AB_PROGNOSE_STATUS
        FROM read_csv({files_sql}, delim=';', header=true, all_varchar=true,
                      union_by_name=true)""")

    n_raw, n_train, n_cancelled = con.execute("""
        SELECT count(*),
               count(*) FILTER (upper(PRODUKT_ID) = 'ZUG'),
               count(*) FILTER (upper(PRODUKT_ID) = 'ZUG'
                                AND coalesce(lower(FAELLT_AUS_TF), 'false') = 'true')
        FROM _raw""").fetchone()

    con.execute("DELETE FROM stg_istdaten WHERE service_day BETWEEN ? AND ?", [first, last])
    con.execute(f"""
        INSERT INTO stg_istdaten
        SELECT CAST(strptime(BETRIEBSTAG, '%d.%m.%Y') AS DATE)      AS service_day,
               FAHRT_BEZEICHNER                                     AS trip_id,
               BETREIBER_ABK                                        AS operator,
               VERKEHRSMITTEL_TEXT                                  AS category,
               try_cast(BPUIC AS BIGINT)                            AS bpuic,
               try_strptime(ANKUNFTSZEIT, {TS_FORMATS})             AS sched_arr,
               try_strptime(ABFAHRTSZEIT, {TS_FORMATS})             AS sched_dep,
               try_strptime(AN_PROGNOSE, {TS_FORMATS})              AS act_arr,
               try_strptime(AB_PROGNOSE, {TS_FORMATS})              AS act_dep,
               AN_PROGNOSE_STATUS IN {_MEASURED_SQL}
                   AND try_strptime(AN_PROGNOSE, {TS_FORMATS}) IS NOT NULL AS arr_measured,
               AB_PROGNOSE_STATUS IN {_MEASURED_SQL}
                   AND try_strptime(AB_PROGNOSE, {TS_FORMATS}) IS NOT NULL AS dep_measured
        FROM _raw
        WHERE upper(PRODUKT_ID) = 'ZUG'
          AND coalesce(lower(FAELLT_AUS_TF), 'false') <> 'true'""")

    n_staged = con.execute(
        "SELECT count(*) FROM stg_istdaten WHERE service_day BETWEEN ? AND ?", [first, last]
    ).fetchone()[0]

    return {
        "rows_raw": n_raw,
        "rows_train": n_train,
        "rows_cancelled": n_cancelled,
        "rows_staged": n_staged,
        "files": len(csv_files),
    }


def build_legs(con, month: str, dim_station_parquet: Path) -> dict:
    """Staged stop events → fct_legs for one month, with quarantine and registries.

    A leg's endpoints join dim_station on the SCD2 validity range — a 2018 event gets the
    2018 coordinates. Unmatched stations are quarantined; foreign stations outside CH_BBOX
    are clipped (expected, ~10% of stop events; counted, not quarantined).
    """
    create_tables(con)
    first, last = month_bounds(month)
    lon_min, lat_min, lon_max, lat_max = CH_BBOX

    con.execute(
        f"CREATE OR REPLACE TEMP VIEW _dim AS SELECT * FROM '{dim_station_parquet.as_posix()}'"
    )

    # Stop order: scheduled time, bpuic as a deterministic tiebreaker. The terminal stop has
    # no departure, so coalesce onto scheduled arrival to keep it in position.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _cand AS
        WITH ev AS (
            SELECT service_day, trip_id, category, bpuic,
                   sched_dep, act_dep, dep_measured, arr_measured,
                   CASE WHEN dep_measured THEN act_dep ELSE sched_dep END AS dep_used,
                   CASE WHEN arr_measured THEN act_arr ELSE sched_arr END AS arr_used,
                   coalesce(sched_dep, sched_arr) AS order_key
            FROM stg_istdaten
            WHERE service_day BETWEEN DATE '{first}' AND DATE '{last}'
        ),
        hop AS (
            SELECT service_day, trip_id, category,
                   bpuic                        AS from_bpuic,
                   lead(bpuic)    OVER w        AS to_bpuic,
                   dep_used,
                   lead(arr_used) OVER w        AS arr_next,
                   dep_measured AND lead(arr_measured) OVER w AS measured,
                   CASE WHEN dep_measured AND sched_dep IS NOT NULL
                        THEN date_diff('second', sched_dep, act_dep) END AS delay_s
            FROM ev
            WINDOW w AS (PARTITION BY trip_id, service_day ORDER BY order_key, bpuic)
        )
        SELECT service_day, trip_id, category, from_bpuic, to_bpuic, measured, delay_s,
               CAST(epoch(timezone('{SOURCE_TZ}', dep_used)) AS BIGINT) AS t_dep,
               CAST(epoch(timezone('{SOURCE_TZ}', arr_next)) AS BIGINT)
                 - CAST(epoch(timezone('{SOURCE_TZ}', dep_used)) AS BIGINT) AS dur
        FROM hop
        WHERE to_bpuic IS NOT NULL""")

    # Tag each candidate exactly once, worst problem first.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _tagged AS
        SELECT c.*,
               f.lon AS f_lon, f.lat AS f_lat, t.lon AS t_lon, t.lat AS t_lat,
               CASE
                 WHEN c.t_dep IS NULL OR c.dur IS NULL       THEN 'missing_time'
                 WHEN c.dur < 0                              THEN 'negative_duration'
                 WHEN c.dur = 0                              THEN 'zero_duration'
                 WHEN f.bpuic IS NULL OR t.bpuic IS NULL     THEN 'unmatched_station'
                 WHEN f.lon NOT BETWEEN {lon_min} AND {lon_max}
                   OR f.lat NOT BETWEEN {lat_min} AND {lat_max}
                   OR t.lon NOT BETWEEN {lon_min} AND {lon_max}
                   OR t.lat NOT BETWEEN {lat_min} AND {lat_max} THEN 'outside_ch'
                 ELSE 'ok'
               END AS verdict
        FROM _cand c
        LEFT JOIN _dim f ON f.bpuic = c.from_bpuic
                        AND c.service_day BETWEEN f.valid_from AND f.valid_to
        LEFT JOIN _dim t ON t.bpuic = c.to_bpuic
                        AND c.service_day BETWEEN t.valid_from AND t.valid_to""")

    # Idempotence: this month's facts and quarantine rows are rebuilt from scratch.
    con.execute("DELETE FROM fct_legs WHERE service_day BETWEEN ? AND ?", [first, last])
    con.execute("DELETE FROM quarantine_legs WHERE service_day BETWEEN ? AND ?", [first, last])

    # outside_ch is an expected clip, not a data defect — count it, don't quarantine it.
    con.execute("""
        INSERT INTO quarantine_legs
        SELECT service_day, trip_id, from_bpuic, to_bpuic, t_dep, dur, verdict
        FROM _tagged WHERE verdict NOT IN ('ok', 'outside_ch')""")

    # Registries append-only, ordered for deterministic ids on a fresh build.
    con.execute("""
        INSERT INTO station_pairs
        SELECT from_bpuic, to_bpuic,
               coalesce((SELECT max(route_id) FROM station_pairs), -1)
                 + row_number() OVER (ORDER BY from_bpuic, to_bpuic)
        FROM (SELECT DISTINCT from_bpuic, to_bpuic FROM _tagged WHERE verdict = 'ok')
        WHERE (from_bpuic, to_bpuic) NOT IN (SELECT from_bpuic, to_bpuic FROM station_pairs)""")
    con.execute("""
        INSERT INTO dim_train_type
        SELECT category,
               coalesce((SELECT max(type_id) FROM dim_train_type), -1)
                 + row_number() OVER (ORDER BY category)
        FROM (SELECT DISTINCT category FROM _tagged WHERE verdict = 'ok' AND category IS NOT NULL)
        WHERE category NOT IN (SELECT category FROM dim_train_type)""")

    n_types = con.execute("SELECT count(*) FROM dim_train_type").fetchone()[0]
    if n_types > 255:
        raise RuntimeError(f"{n_types} train categories no longer fit uint8 (255 = unknown)")

    # The split rule: sub-leg i covers [t_dep + i*3600, ...], all sub-legs flagged synthetic.
    # delay is the run leg's departure delay, carried onto every sub-leg.
    con.execute(f"""
        INSERT INTO fct_legs
        SELECT g.service_day, g.trip_id, p.route_id, g.from_bpuic, g.to_bpuic,
               g.t_dep + {MAX_LEG_DURATION_S} * s.i,
               least({MAX_LEG_DURATION_S}, g.dur - {MAX_LEG_DURATION_S} * s.i),
               coalesce(tt.type_id, 255),
               CAST(greatest(-32768, least(32767, coalesce(g.delay_s, 0))) AS SMALLINT),
               CAST(CASE WHEN g.measured THEN 0 ELSE {FLAG_SCHEDULED_FALLBACK} END
                  | CASE WHEN g.dur > {MAX_LEG_DURATION_S} THEN {FLAG_SYNTHETIC_SPLIT} ELSE 0 END
                  AS TINYINT)
        FROM (SELECT * FROM _tagged WHERE verdict = 'ok') g
        JOIN station_pairs p USING (from_bpuic, to_bpuic)
        LEFT JOIN dim_train_type tt ON tt.category = g.category,
        LATERAL generate_series(
            0, CAST(ceil(g.dur / {MAX_LEG_DURATION_S}.0) AS INT) - 1) s(i)""")

    stats = dict(
        con.execute(f"""
        SELECT verdict, count(*) FROM _tagged GROUP BY verdict
        UNION ALL SELECT 'legs_written', count(*)
        FROM fct_legs WHERE service_day BETWEEN DATE '{first}' AND DATE '{last}'""").fetchall()
    )
    stats.setdefault("ok", 0)
    return stats


def export_day(con, day: date, out_path: Path) -> dict:
    """One UTC calendar day of departures → the 8-byte wire Parquet.

    Keyed by DEPARTURE day (t_dep's UTC date), not service day: the client fetches day N and,
    near midnight, day N−1 — so an 01:30 departure from a late service must sit in its own
    calendar day's file. Sorted by t_dep; row groups ≈ 1 hour of departures (~8-10k rows),
    the measured sweet spot between density and bytes-per-scrub.
    """
    epoch_day = int((day - date(1970, 1, 1)).total_seconds())
    lo, hi = epoch_day, epoch_day + 86400

    n = con.execute(
        "SELECT count(*) FROM fct_legs WHERE t_dep >= ? AND t_dep < ?", [lo, hi]
    ).fetchone()[0]
    if n == 0:
        return {"rows": 0, "bytes": 0}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
            SELECT CAST(route_id AS UINTEGER)  AS route_id,
                   CAST(t_dep    AS UINTEGER)  AS t_dep,
                   CAST(dur      AS USMALLINT) AS dur,
                   CAST(type_id  AS UTINYINT)  AS type,
                   delay,
                   CAST(flags    AS UTINYINT)  AS flags
            FROM fct_legs
            WHERE t_dep >= {lo} AND t_dep < {hi}
            ORDER BY t_dep
        ) TO '{out_path.as_posix()}'
        (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 8192)""")

    size = out_path.stat().st_size
    return {"rows": n, "bytes": size, "bytes_per_leg": round(size / n, 2)}


def month_summary(con, month: str) -> dict:
    """The numbers the M1 gate cares about, for one materialized month."""
    first, last = month_bounds(month)
    row = con.execute(
        """
        SELECT count(*)                                        AS legs,
               count(DISTINCT service_day)                     AS days,
               round(avg(CAST((flags & 1) = 0 AS INT)) * 100, 1) AS pct_measured,
               count(DISTINCT (from_bpuic, to_bpuic))          AS station_pairs,
               count(*) FILTER (flags & 2 > 0)                 AS split_sublegs,
               max(dur)                                        AS max_dur,
               round(quantile_cont(delay, 0.5), 0)             AS delay_p50,
               round(quantile_cont(delay, 0.99), 0)            AS delay_p99
        FROM fct_legs WHERE service_day BETWEEN ? AND ?""",
        [first, last],
    ).fetchone()
    quarantine = dict(
        con.execute(
            """SELECT reason, count(*) FROM quarantine_legs
               WHERE service_day BETWEEN ? AND ? GROUP BY reason ORDER BY 2 DESC""",
            [first, last],
        ).fetchall()
    )
    # Trip-id sanity (open question from the plan): a run with hundreds of stops would mean
    # FAHRT_BEZEICHNER is not unique per day.
    max_stops = con.execute(
        """SELECT max(n) FROM (
               SELECT count(*) AS n FROM stg_istdaten
               WHERE service_day BETWEEN ? AND ? GROUP BY trip_id, service_day)""",
        [first, last],
    ).fetchone()[0]
    return {
        "legs": row[0],
        "days_with_legs": row[1],
        "legs_per_day": round(row[0] / row[1]) if row[1] else 0,
        "pct_measured": row[2],
        "station_pairs_in_month": row[3],
        "split_sublegs": row[4],
        "max_dur": row[5],
        "delay_p50_s": row[6],
        "delay_p99_s": row[7],
        "max_stops_per_run": max_stops,
        "quarantine": quarantine,
    }


def days_in_month(month: str):
    first, last = month_bounds(month)
    d = first
    while d <= last:
        yield d
        d += timedelta(days=1)
