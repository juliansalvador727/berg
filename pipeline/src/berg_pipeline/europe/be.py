"""Belgium adapter: Infrabel's monthly raw punctuality files → normalized stop events.

One CSV per month (about 330 MB, 2M rows): one row per measuring point every passenger train
passed or served on Infrabel's network, with planned and actual arrival/departure times to
the second in Brussels local time (see docs/data-notes-be.md). The raw files stay under
data/datasets/be/raw and never go to R2.

Normalization rules:

  - Commercial stops only. THOP1_COD `D` and `P` are points a train runs through; every
    other code (`=`, NULL for origin/terminus and for every stop of an EXTRA train, and the
    rare coupling/split codes) is a stop. A stop at a ptcar that is not a station or halt
    (a workshop siding, a junction) is operational and dropped the same way.
  - Actual times come from Infrabel's own train detection, so the dataset's semantics are
    `observed`. Each side keeps its own date column, so a train crossing midnight needs no
    inference; local times are converted from Europe/Brussels.
  - The file lists only what ran. Cancelled trains and cancelled stops are simply absent, so
    there is no cancellation capability and a stop skipped by a partial cancellation cannot
    be told from one never scheduled.
  - Only Infrabel's own network is listed: an international train starts or ends at its last
    Belgian point, and its foreign stops are not in the source at all.
  - Stop order is scheduled time. A train number that serves a station twice or whose
    planned times run backwards on one day is two runs sharing a number, or a record the
    source garbled; it is excluded whole and counted, never guessed.
"""

import os
import time
from datetime import date
from pathlib import Path

from berg_pipeline import ingest
from berg_pipeline.europe.config import BELGIUM

MONTHLY_URL = (
    "https://fr.ftp.opendatasoft.com/infrabel/PunctualityHistory/Data_raw_punctuality_{ym}.csv"
)
STATIONS_URL = (
    "https://opendata.infrabel.be/api/explore/v2.1/catalog/datasets/"
    "operationele-punten-van-het-netwerk/exports/csv?delimiter=%3B"
)
# The 2014-2025-07 layout. 2025-08 reorders the columns and 2025-09 adds OP1_COD, so files
# are read by column name (union_by_name) and checked only for the columns used here.
HEADER = (
    "DATDEP,TRAIN_NO,RELATION,TRAIN_SERV,PTCAR_NO,THOP1_COD,LINE_NO_DEP,REAL_TIME_ARR,"
    "REAL_TIME_DEP,PLANNED_TIME_ARR,PLANNED_TIME_DEP,DELAY_ARR,DELAY_DEP,CIRC_TYP,"
    "RELATION_DIRECTION,PTCAR_LG_NM_NL,LINE_NO_ARR,PLANNED_DATE_ARR,PLANNED_DATE_DEP,"
    "REAL_DATE_ARR,REAL_DATE_DEP"
)
REQUIRED_COLUMNS = frozenset({
    "DATDEP", "TRAIN_NO", "RELATION", "TRAIN_SERV", "PTCAR_NO", "THOP1_COD",
    "PLANNED_DATE_ARR", "PLANNED_TIME_ARR", "PLANNED_DATE_DEP", "PLANNED_TIME_DEP",
    "REAL_DATE_ARR", "REAL_TIME_ARR", "REAL_DATE_DEP", "REAL_TIME_DEP",
})
# Measuring points a train passes without stopping.
PASS_CODES = ("D", "P")
# ptcar classes where passengers board. Everything else is a junction, siding or workshop.
PASSENGER_CLASSES = ("Station", "Stop in open track")

# RELATION prefix → the dataset-local category code. "IC 03" and "L 15" carry their line
# number after the prefix; the whole RELATION is kept as the line. Unmapped prefixes keep
# their own name and show as "Other".
CATEGORIES = {
    "IC": "IC",
    "L": "L",
    "P": "P",
    "S": "S",
    "ICE": "ICE",
    "EURST": "EST",
    "EST": "EST",
    "THAL": "THA",
    "TGV": "TGV",
    "INT": "INT",
    "EC": "EC",
    "EN": "EN",
    "NJ": "NJ",
    "ES": "ES",
    "EXTRA": "EXTRA",
    "TRN": "TRN",
}


def raw_path(month: str) -> Path:
    """month is YYYY-MM."""
    return BELGIUM.raw_dir / f"Data_raw_punctuality_{month.replace('-', '')}.csv"


def stations_path() -> Path:
    return BELGIUM.raw_dir / "ptcar.csv"


def _download(url: str, out: Path) -> None:
    """url → out, resuming a partial .part file with a Range request. The server is slow
    (a few hundred KB/s per connection), so an interrupted 330 MB month must not restart.
    """
    import httpx

    part = out.with_name(f"{out.name}.part")
    for attempt in range(1, 11):
        try:
            have = part.stat().st_size if part.exists() else 0
            headers = {"Range": f"bytes={have}-"} if have else {}
            with httpx.stream("GET", url, headers=headers, timeout=300,
                              follow_redirects=True) as response:
                if response.status_code == 416:  # already complete
                    break
                response.raise_for_status()
                if have and response.status_code != 206:
                    have = 0  # server ignored the range; start over
                total = have + int(response.headers.get("content-length", 0))
                out.parent.mkdir(parents=True, exist_ok=True)
                with open(part, "ab" if have else "wb") as fh:
                    for chunk in response.iter_bytes(1 << 20):
                        fh.write(chunk)
            if part.stat().st_size >= total:
                break
        except Exception:
            if attempt == 10:
                raise
            time.sleep(min(60, 2**attempt))
    os.replace(part, out)


def _check_header(path: Path) -> None:
    with open(path, encoding="utf-8-sig") as fh:
        columns = set(fh.readline().strip().split(","))
    if missing := REQUIRED_COLUMNS - columns:
        raise RuntimeError(f"{path.name}: header lacks {sorted(missing)}")


def fetch_month(month: str, force: bool = False) -> Path:
    out = raw_path(month)
    if force or not out.exists():
        _download(MONTHLY_URL.format(ym=month.replace("-", "")), out)
    _check_header(out)
    return out


def fetch_stations(force: bool = False) -> Path:
    out = stations_path()
    if force or not out.exists():
        _download(STATIONS_URL, out)
    return out


# Passenger stops served in 2023-2025 that closed before the ptcar snapshot, which lists only
# today's network. Mortsel-Deurnesteenweg is matched by its TAF/TAP code (BE00864 = ptcar
# 864) in the iRail station list; Baulers is in no Belgian list any more and comes from
# Wikidata. Both lie within 30 m of an OSM rail node.
CLOSED_STATIONS = {
    # ptcarid: (name, code, lat, lon, source)
    125: ("Baulers", "FBLR", 50.607588, 4.344372, "Wikidata Q109037563"),
    864: ("Mortsel-Deurnesteenweg", "GMOD", 51.183023, 4.446514,
          "iRail stations.csv, BE00864"),
}


def write_dim_station(ptcar_csv: Path, out: Path) -> dict:
    """Infrabel's operational points → dim_station (bpuic = ptcarid, the local id).

    The list is the current network with no validity history, so every row is valid for all
    time. Stops that closed before the snapshot come from CLOSED_STATIONS; any other point
    missing from it shows up as an unmatched station. Names are
    shown in both languages where they differ ("Bruxelles-Midi / Brussel-Zuid"), because
    either may be what a user searches for.
    """
    import duckdb

    out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    classes = ", ".join(f"'{c}'" for c in PASSENGER_CLASSES)
    con.execute(f"""
        CREATE TABLE s AS
        SELECT CAST(ptcarid AS BIGINT) AS bpuic,
               CASE WHEN commerciallongnamefrench = commerciallongnamedutch
                         OR commerciallongnamedutch IS NULL
                    THEN commerciallongnamefrench
                    ELSE commerciallongnamefrench || ' / ' || commerciallongnamedutch END AS name,
               symbolicname AS code,
               CAST(split_part(geo_point_2d, ',', 2) AS DOUBLE) AS lon,
               CAST(split_part(geo_point_2d, ',', 1) AS DOUBLE) AS lat,
               class_en IN ({classes}) AS passenger,
               'BE' AS country
        FROM read_csv('{ptcar_csv.as_posix()}', all_varchar=true, header=true, delim=';')
        WHERE geo_point_2d IS NOT NULL""")
    for pid, (name, code, lat, lon, _source) in CLOSED_STATIONS.items():
        if con.execute("SELECT count(*) FROM s WHERE bpuic = ?", [pid]).fetchone()[0] == 0:
            con.execute("INSERT INTO s VALUES (?, ?, ?, ?, ?, true, 'BE')",
                        [pid, name, code, lon, lat])
    n, ids, passenger = con.execute(
        "SELECT count(*), count(DISTINCT bpuic), count(*) FILTER (passenger) FROM s"
    ).fetchone()
    if n != ids:
        raise RuntimeError(f"ptcar list not keyed 1:1 by ptcarid: {n} rows, {ids} ids")
    con.execute(f"""
        COPY (SELECT *, DATE '1900-01-01' AS valid_from, DATE '9999-12-31' AS valid_to
              FROM s ORDER BY bpuic)
        TO '{out.as_posix()}' (FORMAT PARQUET)""")
    return {"points": n, "passenger_points": passenger}


def _utc(day_col: str, time_col: str) -> str:
    """Infrabel's DDMONYYYY date + H:MM:SS local time → epoch seconds UTC. In the repeated
    autumn hour DuckDB takes the later (standard time) instant; no train is scheduled then
    often enough to measure the difference."""
    return (
        f"CAST(epoch(timezone('{BELGIUM.timezone}', "
        f"strptime({day_col} || ' ' || {time_col}, '%d%b%Y %H:%M:%S'))) AS BIGINT)"
    )


# Suburban (S) lines: filed as local trains with a network letter ("L B1-1" is Brussels S1;
# A/C/G/L are Antwerp, Charleroi, Ghent and Liège) until the source starts naming them
# directly ("S1-2", "S61-1") from the 2025-12-14 timetable. "L 15" is an ordinary local train.
S_RELATION = r"^(L [ABCGL][0-9]|S[0-9])"


def _category_sql(expr: str) -> str:
    """RELATION → category. A few trains have no RELATION at all (one on 2023-08-23); they
    get the generic TRAIN rather than a guess."""
    expr = f"coalesce(nullif(trim({expr}), ''), 'TRAIN')"
    cases = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in CATEGORIES.items())
    prefix = f"upper(split_part(trim({expr}), ' ', 1))"
    return (f"CASE WHEN regexp_matches(trim({expr}), '{S_RELATION}') THEN 'S' "
            f"ELSE CASE {prefix} {cases} ELSE {prefix} END END")


def stage_days(con, days: list[date], dim_station_parquet: Path) -> dict:
    """Monthly CSVs → stg_stops for exactly these service days. Idempotent per day."""
    from berg_pipeline.europe import stops

    stops.create_tables(con)
    first, last = min(days), max(days)
    wanted = sorted({f"{d:%Y-%m}" for d in days})
    files = [raw_path(m) for m in wanted]
    missing = [f.name for f in files if not f.exists()]
    if missing:
        raise RuntimeError(f"raw months not fetched: {missing}")
    listed = "[" + ", ".join(f"'{f.as_posix()}'" for f in files) + "]"
    passes = ", ".join(f"'{c}'" for c in PASS_CODES)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _be_rows AS
        SELECT CAST(strptime(DATDEP, '%d%b%Y') AS DATE)       AS service_day,
               TRAIN_NO                                        AS train_number,
               RELATION                                        AS relation,
               TRAIN_SERV                                      AS operator,
               CAST(PTCAR_NO AS BIGINT)                        AS ptcar,
               {_utc('PLANNED_DATE_ARR', 'PLANNED_TIME_ARR')}  AS s_arr,
               {_utc('PLANNED_DATE_DEP', 'PLANNED_TIME_DEP')}  AS s_dep,
               {_utc('REAL_DATE_ARR', 'REAL_TIME_ARR')}        AS a_arr,
               {_utc('REAL_DATE_DEP', 'REAL_TIME_DEP')}        AS a_dep
        FROM read_csv({listed}, all_varchar=true, header=true, delim=',', quote='"',
                      union_by_name=true)
        WHERE CAST(strptime(DATDEP, '%d%b%Y') AS DATE) BETWEEN DATE '{first}' AND DATE '{last}'
          AND (THOP1_COD IS NULL OR THOP1_COD NOT IN ({passes}))""")
    # Operational points (junctions, sidings, workshops) are not stops. A ptcar missing from
    # the snapshot is kept: the leg builder quarantines it as unmatched_station and counts it.
    # Neighbouring stops can share a planned minute, so the actual time breaks ties (it is the
    # order the train really passed them) and the ptcar id makes any remaining tie stable.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _be_stops AS
        SELECT r.*, row_number() OVER (
                   PARTITION BY r.service_day, r.train_number
                   ORDER BY coalesce(r.s_dep, r.s_arr), coalesce(r.s_arr, r.s_dep),
                            coalesce(r.a_arr, r.a_dep), r.ptcar) AS seq
        FROM _be_rows r
        LEFT JOIN '{dim_station_parquet.as_posix()}' d ON d.bpuic = r.ptcar
        WHERE d.bpuic IS NULL OR d.passenger""")

    # One number, one run per day. A repeated station, or an arrival planned before the
    # previous stop's departure, means two runs or a garbled record: excluded whole.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _be_malformed AS
        WITH o AS (
            SELECT service_day, train_number, ptcar, s_arr,
                   lag(coalesce(s_dep, s_arr)) OVER (
                       PARTITION BY service_day, train_number ORDER BY seq) AS prev_dep
            FROM _be_stops
        )
        SELECT service_day, train_number FROM o GROUP BY ALL
        HAVING count(*) FILTER (s_arr < prev_dep) > 0 OR count(*) > count(DISTINCT ptcar)""")

    with ingest._transaction(con):
        # The file lists only trains that ran, so it cannot attest a strike; recorded as zero
        # cancellations so the thin-day check never excuses a Belgian day.
        con.execute("DELETE FROM source_days WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute("""
            INSERT INTO source_days
            SELECT service_day, count(DISTINCT train_number), 0 FROM _be_rows GROUP BY 1""")
        con.execute("DELETE FROM stg_stops WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute(f"""
            INSERT INTO stg_stops
            WITH svc AS (
                SELECT service_day, train_number,
                       any_value(operator) AS operator,
                       arg_min(relation, seq) AS relation
                FROM _be_stops GROUP BY ALL
            )
            SELECT r.service_day,
                   {_category_sql('v.relation')} || ' ' || r.train_number AS trip_id,
                   v.operator,
                   r.train_number,
                   {_category_sql('v.relation')}                      AS category,
                   v.relation                                         AS line,
                   r.ptcar                                            AS station_id,
                   r.seq                                              AS stop_seq,
                   r.s_arr, r.s_dep, r.a_arr, r.a_dep,
                   r.s_arr IS NOT NULL AND r.a_arr IS NOT NULL        AS arr_measured,
                   r.s_dep IS NOT NULL AND r.a_dep IS NOT NULL        AS dep_measured,
                   NULL                                               AS source_revision
            FROM _be_stops r
            JOIN svc v USING (service_day, train_number)
            ANTI JOIN _be_malformed m USING (service_day, train_number)""")

    stats = con.execute("""
        SELECT (SELECT count(*) FROM _be_rows),
               (SELECT count(*) FROM _be_stops),
               (SELECT count(DISTINCT (service_day, train_number)) FROM _be_stops),
               (SELECT count(*) FROM _be_malformed)""").fetchone()
    n_staged, n_unknown = con.execute(
        f"""SELECT count(*), count(*) FILTER (d.bpuic IS NULL)
            FROM stg_stops s LEFT JOIN '{dim_station_parquet.as_posix()}' d
              ON d.bpuic = s.station_id
            WHERE s.service_day BETWEEN ? AND ?""", [first, last]
    ).fetchone()
    return {
        "rows_stopping": stats[0],
        "rows_commercial": stats[1],
        "trains": stats[2],
        "trains_malformed": stats[3],
        "stops_staged": n_staged,
        "stops_unknown_station": n_unknown,
    }
