"""Netherlands adapter: Rijden de Treinen's monthly train archive → normalized stop events.

One gzipped CSV per month, one row per stop of every passenger service, with the scheduled
arrival/departure (RFC 3339 with offset), the last known delay in whole minutes, and
per-side cancellation flags (see docs/data-notes-nl.md). The raw files stay under
data/datasets/nl/raw and never go to R2.

Normalization rules:

  - Trains only. Bus, metro, tram and taxi replacement services ("Stopbus ipv trein",
    "Metro i.p.v. trein", ...) are dropped with the service.
  - A time is scheduled + delay. The source never publishes an absolute actual time, so the
    dataset's semantics are delay_only; a side with no delay value falls back to the
    timetable and is flagged, exactly like an unmeasured Finnish or Swiss side.
  - Completely cancelled services are dropped. A cancelled arrival or departure nulls that
    side of the stop, so the stop builder never draws a leg the source says did not run; a
    stop with neither side left is dropped.
  - Stop order is scheduled time, which for every kept service equals Stop:RDT-ID order.
    Stops added during a diversion are appended with later ids and sometimes leave the
    original route uncancelled; such services are excluded whole and counted, never guessed.
  - A service whose every stop (station, scheduled time) also appears in one other service
    that day is a duplicate record of one train: the archive keeps coupled portions, a train
    that changes number midway (IC 2452 → 3563) and its re-issues (302452) as separate
    services. Train numbers cannot be used to match them. Only the larger — or, when equal,
    the earlier-issued — record is kept.
"""

import os
import time
from datetime import date
from pathlib import Path

from berg_pipeline import ingest
from berg_pipeline.europe.config import NETHERLANDS

BASE = "https://opendata.rijdendetreinen.nl/public"
STATIONS_FILE = "stations-2023-09.csv"
HEADER = (
    "Service:RDT-ID,Service:Date,Service:Type,Service:Company,Service:Train number,"
    "Service:Completely cancelled,Service:Partly cancelled,Service:Maximum delay,Stop:RDT-ID,"
    "Stop:Station code,Stop:Station name,Stop:Arrival time,Stop:Arrival delay,"
    "Stop:Arrival cancelled,Stop:Departure time,Stop:Departure delay,Stop:Departure cancelled,"
    "Stop:Platform change,Stop:Planned platform,Stop:Actual platform"
)
# Replacement services by road or urban rail. Matched case-insensitively on Service:Type.
NON_TRAIN_TYPES = r"bus|ipv|i\.p\.v\."

# Service:Type → the dataset-local category code. Codes are for the UI's service groups and
# the "IC 3563" label; an unmapped type keeps its own name and shows as "Other".
CATEGORIES = {
    "sprinter": "SPR",
    "stoptrein": "ST",
    "sneltrein": "SNT",
    "intercity": "IC",
    "intercity direct": "ICD",
    "eurocity": "EC",
    "eurocity direct": "ECD",
    "ice international": "ICE",
    "ice": "ICE",
    "thalys": "THA",
    "eurostar": "EST",
    "int. trein": "INT",
    "nightjet": "NJ",
    "european sleeper": "ES",
    "nachttrein": "NT",
    "extra trein": "EXTRA",
    "speciale trein": "SPEC",
    "stoomtrein": "STOOM",
}


def raw_path(month: str) -> Path:
    return NETHERLANDS.raw_dir / f"services-{month}.csv.gz"


def stations_path() -> Path:
    return NETHERLANDS.raw_dir / STATIONS_FILE


def _download(url: str, out: Path) -> bool:
    """url → out atomically. False on 404; raises on anything else after retries."""
    import httpx

    tmp = out.with_name(f".{out.name}.tmp")
    for attempt in range(1, 7):
        try:
            with httpx.stream("GET", url, timeout=300, follow_redirects=True) as response:
                if response.status_code == 404:
                    return False
                response.raise_for_status()
                out.parent.mkdir(parents=True, exist_ok=True)
                with open(tmp, "wb") as fh:
                    for chunk in response.iter_bytes(1 << 20):
                        fh.write(chunk)
            os.replace(tmp, out)
            return True
        except Exception:
            if attempt == 6:
                raise
            time.sleep(min(60, 2**attempt))
    raise AssertionError("unreachable")


def _check_header(path: Path) -> None:
    import gzip

    with gzip.open(path, "rt", encoding="utf-8") as fh:
        header = fh.readline().strip()
    if header != HEADER:
        raise RuntimeError(f"{path.name}: unexpected header {header[:120]!r}")


def fetch_month(month: str, force: bool = False) -> Path:
    """One month of services. Months before 2023 exist only inside a yearly file, which is
    downloaded once and split locally, so every later step sees one file per month.
    """
    out = raw_path(month)
    if out.exists() and not force:
        return out
    if not _download(f"{BASE}/services/services-{month}.csv.gz", out):
        year = month[:4]
        yearly = NETHERLANDS.raw_dir / f"services-{year}.csv.gz"
        if not yearly.exists() and not _download(f"{BASE}/services/services-{year}.csv.gz", yearly):
            raise RuntimeError(f"{month}: neither a monthly nor a yearly archive file exists")
        _check_header(yearly)
        import duckdb

        tmp = out.with_name(f".{out.name}.tmp.csv.gz")
        duckdb.sql(f"""
            COPY (SELECT * FROM read_csv('{yearly.as_posix()}', all_varchar=true, header=true)
                  WHERE "Service:Date" LIKE '{month}-%')
            TO '{tmp.as_posix()}' (FORMAT CSV, HEADER, COMPRESSION gzip)""")
        os.replace(tmp, out)
    _check_header(out)
    return out


def fetch_stations(force: bool = False) -> Path:
    out = stations_path()
    if force or not out.exists():
        if not _download(f"{BASE}/stations/{STATIONS_FILE}", out):
            raise RuntimeError(f"{STATIONS_FILE} not found")
    return out


def write_dim_station(stations_csv: Path, out: Path) -> dict:
    """The archive's station list → dim_station (bpuic = the UIC code, the local id).

    The list is a 2023-09 snapshot with no validity history, so every row is valid for all
    time. It also carries foreign stations (country D, B, F, ...), which the country clip
    keeps off the Dutch map.
    """
    import duckdb

    out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE s AS
        SELECT CAST(uic AS BIGINT) AS bpuic, name_long AS name, code,
               CAST(geo_lng AS DOUBLE) AS lon, CAST(geo_lat AS DOUBLE) AS lat,
               true AS passenger, country
        FROM read_csv('{stations_csv.as_posix()}', all_varchar=true, header=true)""")
    n, ids, codes = con.execute(
        "SELECT count(*), count(DISTINCT bpuic), count(DISTINCT code) FROM s"
    ).fetchone()
    if not n == ids == codes:
        raise RuntimeError(f"station list not keyed 1:1 by uic and code: {n}, {ids}, {codes}")
    con.execute(f"""
        COPY (SELECT *, DATE '1900-01-01' AS valid_from, DATE '9999-12-31' AS valid_to
              FROM s ORDER BY bpuic)
        TO '{out.as_posix()}' (FORMAT PARQUET)""")
    return {"stations": n}


def _epoch(expr: str) -> str:
    """RFC 3339 with offset → epoch seconds. The offset is honoured by TIMESTAMPTZ."""
    return f"CAST(epoch(CAST({expr} AS TIMESTAMPTZ)) AS BIGINT)"


def _category_sql(expr: str) -> str:
    cases = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in CATEGORIES.items())
    return f"CASE lower(trim({expr})) {cases} ELSE trim({expr}) END"


def stage_days(con, days: list[date], dim_station_parquet: Path) -> dict:
    """Monthly CSVs → stg_stops for exactly these service days. Idempotent per day."""
    from berg_pipeline.europe import stops

    stops.create_tables(con)
    first, last = min(days), max(days)
    wanted = sorted({f"{d:%Y-%m}" for d in days})
    files = [raw_path(m) for m in wanted]
    missing = [f.name for f in files if not f.exists()]
    if missing:
        raise RuntimeError(f"raw archive months not fetched: {missing}")
    listed = "[" + ", ".join(f"'{f.as_posix()}'" for f in files) + "]"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _nl_rows AS
        SELECT CAST("Service:RDT-ID" AS BIGINT)                AS sid,
               CAST("Service:Date" AS DATE)                    AS service_day,
               "Service:Type"                                  AS service_type,
               "Service:Company"                               AS company,
               "Service:Train number"                          AS train_number,
               coalesce("Service:Completely cancelled" = 'true', false) AS svc_cancelled,
               CAST("Stop:RDT-ID" AS BIGINT)                   AS stop_rid,
               "Stop:Station code"                             AS code,
               {_epoch('"Stop:Arrival time"')}                 AS s_arr,
               TRY_CAST("Stop:Arrival delay" AS INTEGER)       AS arr_delay,
               coalesce("Stop:Arrival cancelled" = 'true', false) AS arr_cancelled,
               {_epoch('"Stop:Departure time"')}               AS s_dep,
               TRY_CAST("Stop:Departure delay" AS INTEGER)     AS dep_delay,
               coalesce("Stop:Departure cancelled" = 'true', false) AS dep_cancelled
        FROM read_csv({listed}, all_varchar=true, header=true)
        WHERE CAST("Service:Date" AS DATE) BETWEEN DATE '{first}' AND DATE '{last}'
          AND NOT regexp_matches(lower("Service:Type"), '{NON_TRAIN_TYPES}')""")

    # Diversion stops are appended with later ids than the route they replace, sometimes
    # without cancelling it; a repeated station is the same symptom. Excluded whole.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _nl_malformed AS
        WITH o AS (
            SELECT sid, code, coalesce(s_arr, s_dep) AS t_in,
                   lag(coalesce(s_dep, s_arr)) OVER (PARTITION BY sid ORDER BY stop_rid) AS t_prev
            FROM _nl_rows WHERE s_arr IS NOT NULL OR s_dep IS NOT NULL
        )
        SELECT sid FROM o GROUP BY sid
        HAVING count(*) FILTER (t_in < t_prev) > 0 OR count(*) > count(DISTINCT code)""")

    # Duplicate records of one train: every (station, scheduled time) of S also appears in S2,
    # and S2 is larger, or equal and issued first. Two distinct trains cannot share a station
    # and minute at every stop; coupled portions do, and draw as one consist.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _nl_redundant AS
        WITH sig AS (
            SELECT sid, service_day, code, coalesce(s_dep, s_arr) AS t
            FROM _nl_rows WHERE s_arr IS NOT NULL OR s_dep IS NOT NULL
        ),
        size AS (SELECT sid, count(*) AS n FROM sig GROUP BY sid),
        overlap AS (
            SELECT a.sid, b.sid AS other, count(*) AS m
            FROM sig a JOIN sig b
              ON a.service_day = b.service_day AND a.code = b.code AND a.t = b.t
             AND a.sid <> b.sid
            GROUP BY a.sid, b.sid
        )
        SELECT DISTINCT o.sid
        FROM overlap o JOIN size sa ON sa.sid = o.sid JOIN size sb ON sb.sid = o.other
        WHERE o.m = sa.n AND (sb.n > sa.n OR (sb.n = sa.n AND o.other < o.sid))""")

    with ingest._transaction(con):
        # What the source itself says about each day, for the thin-day check: a day where
        # nearly every service is cancelled (a strike) is thin because the railway stopped.
        con.execute("DELETE FROM source_days WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute("""
            INSERT INTO source_days
            SELECT service_day, count(*), count(*) FILTER (svc_cancelled)
            FROM (SELECT sid, any_value(service_day) AS service_day,
                         bool_and(svc_cancelled) AS svc_cancelled
                  FROM _nl_rows GROUP BY sid)
            GROUP BY service_day""")
        con.execute("DELETE FROM stg_stops WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute(f"""
            INSERT INTO stg_stops
            WITH live AS (
                SELECT r.*,
                       r.s_arr IS NOT NULL AND NOT r.arr_cancelled AS arr_live,
                       r.s_dep IS NOT NULL AND NOT r.dep_cancelled AS dep_live
                FROM _nl_rows r
                ANTI JOIN _nl_malformed USING (sid)
                ANTI JOIN _nl_redundant USING (sid)
                WHERE NOT r.svc_cancelled
            ),
            svc AS (
                SELECT sid, any_value(service_day) AS service_day,
                       arg_min(train_number, stop_rid) AS number,
                       any_value(company) AS company,
                       {_category_sql('any_value(service_type)')} AS category
                FROM live GROUP BY sid
            ),
            labelled AS (
                SELECT *, category || ' ' || number AS label,
                       row_number() OVER (PARTITION BY service_day, category || ' ' || number
                                          ORDER BY sid) AS k
                FROM svc
            )
            SELECT l.service_day,
                   -- (service_day, trip_id) is the journey key, so a second service with the
                   -- same number that day (a split portion) needs its own id.
                   CASE WHEN l.k = 1 THEN l.label ELSE l.label || ' #' || l.k END AS trip_id,
                   l.company                                      AS operator,
                   l.number                                       AS train_number,
                   l.category,
                   l.label                                        AS line,
                   coalesce(d.bpuic, -CAST(hash(r.code) % 1000000000 AS BIGINT) - 1) AS station_id,
                   row_number() OVER (PARTITION BY r.sid ORDER BY r.stop_rid) AS stop_seq,
                   CASE WHEN r.arr_live THEN r.s_arr END           AS sched_arr,
                   CASE WHEN r.dep_live THEN r.s_dep END           AS sched_dep,
                   CASE WHEN r.arr_live THEN r.s_arr + 60 * r.arr_delay END AS act_arr,
                   CASE WHEN r.dep_live THEN r.s_dep + 60 * r.dep_delay END AS act_dep,
                   r.arr_live AND r.arr_delay IS NOT NULL          AS arr_measured,
                   r.dep_live AND r.dep_delay IS NOT NULL          AS dep_measured,
                   r.sid                                           AS source_revision
            FROM live r
            JOIN labelled l USING (sid)
            LEFT JOIN '{dim_station_parquet.as_posix()}' d ON d.code = r.code
            WHERE r.arr_live OR r.dep_live""")

    stats = con.execute("""
        SELECT (SELECT count(DISTINCT sid) FROM _nl_rows),
               (SELECT count(DISTINCT sid) FROM _nl_rows WHERE svc_cancelled),
               (SELECT count(*) FROM _nl_malformed),
               (SELECT count(*) FROM _nl_redundant),
               (SELECT count(*) FROM _nl_rows WHERE arr_cancelled OR dep_cancelled)""").fetchone()
    n_staged, n_unmatched = con.execute(
        "SELECT count(*), count(*) FILTER (station_id < 0) FROM stg_stops "
        "WHERE service_day BETWEEN ? AND ?", [first, last]
    ).fetchone()
    return {
        "services_train": stats[0],
        "services_cancelled": stats[1],
        "services_malformed": stats[2],
        "services_duplicate": stats[3],
        "rows_cancelled_side": stats[4],
        "stops_staged": n_staged,
        "stops_unknown_station": n_unmatched,
    }
