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
import codecs
import json
from datetime import date, timedelta
from pathlib import Path

from berg_pipeline.constants import (
    CH_BBOX,
    FLAG_SCHEDULED_FALLBACK,
    FLAG_SYNTHETIC_SPLIT,
    MAX_LEG_DURATION_S,
    MAX_RAW_LEG_DURATION_S,
    MEASURED_STATUSES,
    MIN_LEGS_PER_DAY,
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
            dep_measured BOOLEAN   NOT NULL,
            -- Cheap here, unrecoverable later: the raw CSVs are deleted after staging, so a
            -- column not taken now costs a 1.27 TB re-download to add. Be greedy at this
            -- boundary; the wire format downstream is where bytes are fought for.
            line           VARCHAR,  -- LINIEN_TEXT: 'S3', 'IC 8' — the line, not the category
            umlauf_id      VARCHAR,  -- UMLAUF_ID: rolling-stock rotation
            is_extra       BOOLEAN,  -- ZUSATZFAHRT_TF: unscheduled/relief run
            is_passthrough BOOLEAN   -- DURCHFAHRT_TF: passes through without stopping
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
            flags       TINYINT  NOT NULL,
            -- The trip's line ('S3'), not the leg's. Never goes on the wire: it belongs to a
            -- journey, so it rides in the journeys sidecar where ~500 distinct values
            -- dictionary-encode to almost nothing. legs stays 8 bytes.
            line        VARCHAR
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


# Columns added after months were already staged. ALTER is idempotent, so an existing
# berg.duckdb keeps its rows and gains NULLs, and a month re-ingested later fills them in.
# NULL here means "staged before this column existed", not "absent from the source".
_ADDED_COLUMNS = (
    ("stg_istdaten", "line", "VARCHAR"),
    ("stg_istdaten", "umlauf_id", "VARCHAR"),
    ("stg_istdaten", "is_extra", "BOOLEAN"),
    ("stg_istdaten", "is_passthrough", "BOOLEAN"),
    ("fct_legs", "line", "VARCHAR"),
)


def migrate_tables(con) -> None:
    """Bring an existing database up to the current schema without rebuilding it."""
    for table, column, type_ in _ADDED_COLUMNS:
        con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {type_}")


_COUNT_SQL = """
    SELECT count(*),
           count(*) FILTER (upper(PRODUKT_ID) = 'ZUG'),
           count(*) FILTER (upper(PRODUKT_ID) = 'ZUG'
                            AND coalesce(lower(FAELLT_AUS_TF), 'false') = 'true')
    FROM _raw"""

# The archive is not consistently UTF-8, and encoding varies FILE TO FILE INSIDE ONE MONTH:
# 2018-11-01.csv is UTF-8, 2018-11-02.csv is latin-1. So there is no per-month encoding to
# pick — read_csv takes one encoding per call, and either choice fails half the month.
#
# Nor is latin-1 a safe catch-all: DuckDB validates it, and rejects the UTF-8 file outright.
# Both must be honoured, so group the files and UNION ALL the groups.
#
# Every offending byte is in a column this pipeline discards — BETREIBER_NAME
# ('Baden-Württemberg'), HALTESTELLEN_NAME ('Möhlin', 'Bossière') — because operator comes
# from BETREIBER_ABK and station names from dim_station. A month died over umlauts it was
# going to throw away.
_RAW_COLUMNS = """BETRIEBSTAG, FAHRT_BEZEICHNER, BETREIBER_ABK, VERKEHRSMITTEL_TEXT,
                  PRODUKT_ID, FAELLT_AUS_TF, BPUIC,
                  ANKUNFTSZEIT, AN_PROGNOSE, AN_PROGNOSE_STATUS,
                  ABFAHRTSZEIT, AB_PROGNOSE, AB_PROGNOSE_STATUS,
                  LINIEN_TEXT, UMLAUF_ID, ZUSATZFAHRT_TF, DURCHFAHRT_TF"""


def file_encoding(path: Path) -> str:
    """'utf-8' or 'latin-1', decided by decoding the whole file.

    Whole file, not a sample: a lone umlaut anywhere flips the verdict, and guessing wrong
    means either a failed month (guessed utf-8) or silent mojibake (guessed latin-1). The
    latin-1 case exits at the first bad byte, so only genuine UTF-8 files are read through.
    """
    decoder = codecs.getincrementaldecoder("utf-8")()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 20):
            try:
                decoder.decode(chunk)
            except UnicodeDecodeError:
                return "latin-1"
    try:
        decoder.decode(b"", final=True)
    except UnicodeDecodeError:
        return "latin-1"
    return "utf-8"


def _stage_raw_view(con, csv_files: list[Path]) -> dict[str, int]:
    """Define _raw over the month's CSVs, one read_csv per encoding. Returns the file split."""
    groups: dict[str, list[Path]] = {}
    for p in csv_files:
        groups.setdefault(file_encoding(p), []).append(p)

    selects = []
    for encoding, files in sorted(groups.items()):
        listed = "[" + ", ".join(f"'{p.as_posix()}'" for p in files) + "]"
        selects.append(f"""
            SELECT {_RAW_COLUMNS}
            FROM read_csv({listed}, delim=';', header=true, all_varchar=true,
                          union_by_name=true, null_padding=true, encoding='{encoding}')""")
    con.execute("CREATE OR REPLACE TEMP VIEW _raw AS " + " UNION ALL ".join(selects))
    return {enc: len(files) for enc, files in sorted(groups.items())}


def stage_month(con, month: str, csv_files: list[Path]) -> dict:
    """Daily CSVs → normalized train stop events for one month.

    all_varchar because autodetect silently typed BETRIEBSTAG as DATE on a real file;
    union_by_name because SLOID appears in 2025-11 and files within a month could straddle it.
    null_padding because the archive ships truncated rows: 2024-10-26.csv ends mid-row, one
    short line in 1.6M, and strict mode failed the whole month over it. Padding degrades
    gracefully — a truncated row gets NULL trailing columns, so a bus is dropped by the Zug
    filter and a train would land in scheduled-fallback or quarantine rather than killing a
    month. Do NOT reach for ignore_errors instead; it discards rows silently.
    Cancelled stops are dropped (counted here) — decided at M1; revisit if ghost trains ever
    become a feature.
    """
    create_tables(con)
    migrate_tables(con)
    first, last = month_bounds(month)

    encodings = _stage_raw_view(con, csv_files)
    n_raw, n_train, n_cancelled = con.execute(_COUNT_SQL).fetchone()

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
                   AND try_strptime(AB_PROGNOSE, {TS_FORMATS}) IS NOT NULL AS dep_measured,
               nullif(trim(LINIEN_TEXT), '')                         AS line,
               nullif(trim(UMLAUF_ID), '')                          AS umlauf_id,
               lower(ZUSATZFAHRT_TF) = 'true'                       AS is_extra,
               lower(DURCHFAHRT_TF) = 'true'                        AS is_passthrough
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
        # e.g. {'latin-1': 29, 'utf-8': 1} — visible, because a month whose encoding split
        # shifts is the archive telling you something.
        "encodings": encodings,
    }


def build_legs(con, month: str, dim_station_parquet: Path) -> dict:
    """Staged stop events → fct_legs for one month, with quarantine and registries.

    A leg's endpoints join dim_station on the SCD2 validity range — a 2018 event gets the
    2018 coordinates. Unmatched stations are quarantined; foreign stations outside CH_BBOX
    are clipped (expected, ~10% of stop events; counted, not quarantined).
    """
    create_tables(con)
    migrate_tables(con)
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
            SELECT service_day, trip_id, category, bpuic, line,
                   sched_dep, act_dep, sched_arr, act_arr, dep_measured, arr_measured,
                   coalesce(sched_dep, sched_arr) AS order_key
            FROM stg_istdaten
            WHERE service_day BETWEEN DATE '{first}' AND DATE '{last}'
        ),
        hop AS (
            -- line comes from the departure stop, not lead(): it is a property of the run, so
            -- it is constant within the window, and the origin's value is the leg's own.
            --
            -- ONE CLOCK PER LEG. dur is a difference, so both ends must come from the same
            -- basis; picking each end's best available time independently subtracts a scheduled
            -- arrival from an actual departure. That is not a smaller error, it is a different
            -- quantity: a late train's actual departure routinely lands AFTER the scheduled
            -- arrival of its next stop, so the leg goes negative and is quarantined as a source
            -- error it never was. Measured against the archive: legs sharing a basis are 0.0%
            -- negative (2 of 709,048), mixed legs are 33.6% negative — and the mixed legs that
            -- stay positive are worse, because they publish a plausible duration built from two
            -- clocks. ~6M of them are live. The 1.34% both-measured rate is the real source
            -- error; everything above it was manufactured here.
            SELECT service_day, trip_id, category, line,
                   bpuic                        AS from_bpuic,
                   lead(bpuic)    OVER w        AS to_bpuic,
                   dep_measured AND lead(arr_measured) OVER w AS measured,
                   CASE WHEN dep_measured AND lead(arr_measured) OVER w
                        THEN act_dep ELSE sched_dep END AS dep_used,
                   CASE WHEN dep_measured AND lead(arr_measured) OVER w
                        THEN lead(act_arr) OVER w ELSE lead(sched_arr) OVER w END AS arr_next,
                   -- Departure delay stands on its own: it needs only this stop's two times,
                   -- so it survives a leg falling back to the timetable for its geometry.
                   CASE WHEN dep_measured AND sched_dep IS NOT NULL
                        THEN date_diff('second', sched_dep, act_dep) END AS delay_s
            FROM ev
            WINDOW w AS (PARTITION BY trip_id, service_day ORDER BY order_key, bpuic)
        )
        SELECT service_day, trip_id, category, line, from_bpuic, to_bpuic, measured, delay_s,
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
                 -- Before the split rule, not after: past this point dur is an amplification
                 -- factor, and an unbounded one turns a single broken timestamp into millions
                 -- of sub-legs. See MAX_RAW_LEG_DURATION_S.
                 WHEN c.dur > {MAX_RAW_LEG_DURATION_S}       THEN 'absurd_duration'
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
                  AS TINYINT),
               g.line
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


def export_journeys_day(con, day: date, out_path: Path) -> dict:
    """One UTC calendar day of departures → the click-detail sidecar.

    Additive by design: the 8-byte leg wire stays exactly as it is, and this pays for itself
    only when someone actually clicks. Identity is deliberately NOT in the leg file — a
    journey id on every leg taxes 460M rows to answer a question asked a few times a session.

    Same WHERE and same ORDER BY as export_day, so row N here is leg N there. The client does
    not rely on that (it looks up by t_dep + route_id, which prunes on the sorted t_dep), but
    keeping them aligned makes the two files diffable when something looks wrong.

    Carries no station ids: (from, to) is a pure function of route_id, published once in
    static/route_pairs.json rather than repeated 460M times.

    service_day is here because it is NOT the file's date — a 00:30 departure belongs to the
    previous service day, so both appear in one file — and (trip_id, service_day) is the
    journey key the leg-building window partitions by. trip_id alone would merge two trains.
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
            SELECT CAST(t_dep    AS UINTEGER) AS t_dep,
                   CAST(route_id AS UINTEGER) AS route_id,
                   trip_id,
                   service_day,
                   line
            FROM fct_legs
            WHERE t_dep >= {lo} AND t_dep < {hi}
            ORDER BY t_dep
        ) TO '{out_path.as_posix()}'
        (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 8192)""")

    size = out_path.stat().st_size
    return {"rows": n, "bytes": size, "bytes_per_leg": round(size / n, 2)}


def export_train_types(con, out_path: Path) -> dict:
    """type_id → category ('S', 'IC', 'RE') at static/train_types.json.

    The wire carries type as a uint8; without this the client has a number and no meaning, so
    every train renders the same colour. Ids are append-only, so this file only ever grows —
    but it must be republished when it does, or new categories decode as unknown.
    """
    rows = con.execute("SELECT type_id, category FROM dim_train_type ORDER BY type_id").fetchall()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({str(t): c for t, c in rows}, separators=(",", ":")))
    return {"types": len(rows), "bytes": out_path.stat().st_size}


def export_route_pairs(con, out_path: Path) -> dict:
    """route_id → (from_bpuic, to_bpuic), once. Joins to stations.json for names.

    Small enough (one row per station pair, not per leg) to ship as JSON and keep in memory,
    which is what lets a hover show "Bern → Thun" without touching the sidecar at all.
    """
    rows = con.execute(
        "SELECT route_id, from_bpuic, to_bpuic FROM station_pairs ORDER BY route_id"
    ).fetchall()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({str(r[0]): [r[1], r[2]] for r in rows}, separators=(",", ":")))
    return {"pairs": len(rows), "bytes": out_path.stat().st_size}


def validate_month_days(con, month: str) -> list[tuple[str, int]]:
    """Every censused-usable day of `month` must clear MIN_LEGS_PER_DAY. Returns the failures.

    This is the check whose absence let 2023-09 publish 28 days of 24 legs each and record
    them in manifest.json as successes. Nothing upstream caught it: raw_zip's _assert_complete
    only counts day CSVs, and export_day happily writes any file with rows > 0 — so a month
    that staged almost nothing looked exactly like a good one from there on.

    Counted by DEPARTURE day, matching export_day's window, because that is the unit that
    ships. The month's trailing boundary day is deliberately not checked here: build_month
    exports it while only this month is staged, so it holds just the last night's post-midnight
    departures and is legitimately tiny. It gets its real export — and its check — when the
    next month runs.
    """
    from berg_pipeline import archive

    absent = archive.expected_absent_days(month)
    if archive.expected_usable_days(month) is None:
        return []  # uncensused: no ground truth to check against

    bad = []
    for day in days_in_month(month):
        if day in absent:
            continue  # a genuine archive hole yields nothing, correctly
        epoch_day = int((day - date(1970, 1, 1)).total_seconds())
        n = con.execute(
            "SELECT count(*) FROM fct_legs WHERE t_dep >= ? AND t_dep < ?",
            [epoch_day, epoch_day + 86400],
        ).fetchone()[0]
        if n < MIN_LEGS_PER_DAY:
            bad.append((day.isoformat(), n))
    return bad


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
