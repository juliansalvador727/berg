"""Austria adapter: ÖBB train runs + ÖBB timetable → normalized stop events.

Two open sources, joined by train number and operating day:

  - ÖBB-Infrastruktur's "Zugfahrten" (EU Delegated Regulation 2024/490 data, CC BY 3.0 AT):
    one row per train run with the planned and actual time, to the second, at the first and
    last operating point ÖBB recorded for it. Those points are operating points, not passenger
    termini: yards ("Ow", Wien Westbf Fbf), junctions ("Mlx") and track groups ("Nbn"). The
    weekly files are published in one rolling ZIP; each week is kept under raw/zugfahrten.
  - ÖBB-Personenverkehr's GTFS timetable (CC BY 4.0): stops, stop order, scheduled times and
    stop coordinates. It starts with the 2025-12-14 timetable change.

Normalization rules:

  - A published train is a timetabled trip that ÖBB recorded running that day. A trip with no
    run is not published (the run file is the only evidence it ran), and a run with no trip
    (empty stock, freight, other operators) has no stops to draw.
  - Trip and run are matched one-to-one on (train number, operating day). Where a number has
    several trips or runs that day, the pair whose scheduled time spans overlap most wins.
  - The run gives two observed delays: dA at its first point's planned time tA, dB at its last
    point's planned time tB. An observation more than 20 min early or 12 h late is a
    registration or dating artefact, not a running time, and is discarded. A stop scheduled at time s gets dA before tA, dB after tB, and the
    linear interpolation between. One missing observation lends the other to every stop; a run
    with neither is not published. Dataset semantics: delay_interpolated.
  - Pass-through timetable rows (no pickup and no drop-off) are not commercial stops and are
    dropped. A dwell the interpolation would make negative is clamped to zero.
"""

import os
import re
import time
import zipfile
import zlib
from datetime import date
from pathlib import Path

from berg_pipeline import ingest
from berg_pipeline.europe.config import AUSTRIA

RUNS_URL = "https://static.web.oebb.at/open-data/infra/delVO_mmtis/mmtis_zugfahrten.zip"
GTFS_URL = "https://static.web.oebb.at/open-data/soll-fahrplan-gtfs/GTFS_Fahrplan_{year}.zip"
GTFS_YEARS = (2026,)
RUNS_HEADER = (
    "zugnummer,betriebstag,abfahrtzeit_soll,abfahrtzeit_ist,start_betriebsstelle,"
    "ankunftzeit_soll,ankunftzeit_ist,ziel_betriebsstelle,fahrzeit_delta"
)
TZ = AUSTRIA.timezone

# An observation outside [MIN_DELAY_S, MAX_DELAY_S] is not a running time and is discarded;
# the run's other one is used. Measured over 1.57M runs: early departures fall off smoothly
# to about -10 min (timetable padding), then a thin tail runs to hours and days before the
# plan (a train registered at 00:08 for a 05:46 start, or stamped with a date days earlier):
# 924 departures and 592 arrivals are more than 20 min early. Three are more than 12 h late.
MIN_DELAY_S = -20 * 60
MAX_DELAY_S = 12 * 3600

# A trip and a run with the same number and day must overlap in scheduled time by at least
# this much to be the same train; the run's points sit just inside the trip's termini.
MIN_OVERLAP_S = -15 * 60

# trip_short_name prefix → dataset-local category code (the UI's service groups).
CATEGORIES = {
    "S": "S", "R": "R", "REX": "REX", "CJX": "CJX", "IR": "IR", "IC": "IC", "D": "D",
    "EC": "EC", "RJ": "RJ", "RJX": "RJX", "ICE": "ICE", "NJ": "NJ", "EN": "EN", "CAT": "CAT",
}


def raw_runs_dir() -> Path:
    return AUSTRIA.raw_dir / "zugfahrten"


def gtfs_dir(year: int) -> Path:
    return AUSTRIA.raw_dir / f"GTFS_Fahrplan_{year}"


def _download(url: str, out: Path) -> None:
    import httpx

    tmp = out.with_name(f".{out.name}.tmp")
    for attempt in range(1, 7):
        try:
            with httpx.stream("GET", url, timeout=300, follow_redirects=True) as response:
                response.raise_for_status()
                out.parent.mkdir(parents=True, exist_ok=True)
                with open(tmp, "wb") as fh:
                    for chunk in response.iter_bytes(1 << 20):
                        fh.write(chunk)
            os.replace(tmp, out)
            return
        except Exception:
            if attempt == 6:
                raise
            time.sleep(min(60, 2**attempt))


def fetch_runs() -> dict:
    """Download today's rolling ZIP and file every weekly CSV under raw/zugfahrten.

    The ZIP drops old weeks as new ones arrive, so each snapshot is kept and every week ever
    seen stays on disk. A week present in the new snapshot replaces the stored copy.
    """
    snap = AUSTRIA.raw_dir / "snapshots" / f"mmtis_zugfahrten-{date.today():%Y-%m-%d}.zip"
    if not snap.exists():
        _download(RUNS_URL, snap)
    written = 0
    with zipfile.ZipFile(snap) as zf:
        for name in zf.namelist():
            if not name.endswith(".csv"):
                continue
            out = raw_runs_dir() / name
            out.parent.mkdir(parents=True, exist_ok=True)
            data = zf.read(name)
            if name.endswith("_mmtis_zugfahrten.csv"):
                header = data.decode("utf-8-sig").split("\n", 1)[0].strip()
                if header != RUNS_HEADER:
                    raise RuntimeError(f"{name}: unexpected header {header[:120]!r}")
            if not out.exists() or out.read_bytes() != data:
                tmp = out.with_name(f".{out.name}.tmp")
                tmp.write_bytes(data)
                os.replace(tmp, out)
                written += 1
    return {"snapshot": snap.name, "weeks_written": written}


def fetch_gtfs(force: bool = False) -> list[Path]:
    dirs = []
    for year in GTFS_YEARS:
        zpath = AUSTRIA.raw_dir / f"GTFS_Fahrplan_{year}.zip"
        if force or not zpath.exists():
            _download(GTFS_URL.format(year=year), zpath)
        out = gtfs_dir(year)
        if not (out / "stop_times.txt").exists():
            with zipfile.ZipFile(zpath) as zf:
                for member in zf.namelist():
                    if member.endswith(".txt") and not member.endswith("shapes.txt"):
                        target = out / Path(member).name
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(zf.read(member))
        dirs.append(out)
    return dirs


def station_id(ifopt: str) -> int:
    """IFOPT stop place → dataset-local id. Austrian ids are region * 100000 + number, so they
    read back; foreign ones get a stable id from their CRC above every Austrian id."""
    m = re.fullmatch(r"at:(\d+):(\d+)", ifopt)
    if m and int(m[2]) < 100_000:
        return int(m[1]) * 100_000 + int(m[2])
    return 10_000_000_000 + zlib.crc32(ifopt.encode())


def write_dim_station(gtfs_dirs: list[Path], out: Path) -> dict:
    """GTFS stop places (location_type 1, or stops with no parent) → dim_station.

    bpuic is station_id(ifopt); country is the IFOPT prefix, so the country clip keeps
    cross-border legs of international trains off the Austrian map.
    """
    import duckdb

    con = duckdb.connect()
    listed = "[" + ", ".join(f"'{(d / 'stops.txt').as_posix()}'" for d in gtfs_dirs) + "]"
    # Every stop row (parent, platform, entrance) collapses to its stop place, the first three
    # IFOPT parts; the parent's own position wins where there is one.
    rows = con.execute(f"""
        SELECT {_ifopt_sql("regexp_replace(stop_id, '^P', '')")} AS ifopt,
               coalesce(any_value(stop_name) FILTER (location_type = '1'), any_value(stop_name)),
               coalesce(avg(CAST(stop_lon AS DOUBLE)) FILTER (location_type = '1'),
                        avg(CAST(stop_lon AS DOUBLE))),
               coalesce(avg(CAST(stop_lat AS DOUBLE)) FILTER (location_type = '1'),
                        avg(CAST(stop_lat AS DOUBLE)))
        FROM read_csv({listed}, all_varchar=true, header=true)
        GROUP BY 1""").fetchall()
    con.execute("""CREATE TABLE s (bpuic BIGINT, ifopt VARCHAR, name VARCHAR, code VARCHAR,
                                   lon DOUBLE, lat DOUBLE, passenger BOOLEAN, country VARCHAR)""")
    con.executemany(
        "INSERT INTO s VALUES (?, ?, ?, NULL, ?, ?, true, ?)",
        [(station_id(i), i, n, lo, la, i.split(":")[0].upper()) for i, n, lo, la in rows],
    )
    n, ids = con.execute("SELECT count(*), count(DISTINCT bpuic) FROM s").fetchone()
    if n != ids:
        raise RuntimeError(f"station ids collide: {n} stops, {ids} ids")
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (SELECT *, DATE '1900-01-01' AS valid_from, DATE '9999-12-31' AS valid_to
              FROM s ORDER BY bpuic)
        TO '{out.as_posix()}' (FORMAT PARQUET)""")
    return {"stations": n, "austrian": sum(1 for r in rows if r[0].startswith("at:"))}


def _ifopt_sql(expr: str) -> str:
    """A GTFS stop id ('at:44:44900:0:22') → its stop place ('at:44:44900')."""
    return f"coalesce(nullif(regexp_extract({expr}, '^([a-z]+:\\d+:\\d+)', 1), ''), {expr})"


def _ts(expr: str) -> str:
    """'2026-02-16T17:15:06+0100' → epoch seconds; the offset is honoured."""
    return f"CAST(epoch(strptime({expr}, '%Y-%m-%dT%H:%M:%S%z')) AS BIGINT)"


def _gtfs_secs(expr: str) -> str:
    """GTFS 'HH:MM:SS' (hours may exceed 23) → seconds after the service day's noon - 12 h."""
    return (f"(CAST(split_part({expr}, ':', 1) AS INTEGER) * 3600 "
            f"+ CAST(split_part({expr}, ':', 2) AS INTEGER) * 60 "
            f"+ CAST(split_part({expr}, ':', 3) AS INTEGER))")


def _category_sql(expr: str) -> str:
    cases = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in CATEGORIES.items())
    return f"CASE {expr} {cases} ELSE coalesce({expr}, 'OTHER') END"


def stage_days(con, days: list[date], dim_station_parquet: Path, gtfs_dirs: list[Path]) -> dict:
    """Runs + timetable → stg_stops for exactly these service days. Idempotent per day."""
    from berg_pipeline.europe import stops

    stops.create_tables(con)
    first, last = min(days), max(days)
    runs_glob = (raw_runs_dir() / "*" / "*_mmtis_zugfahrten.csv").as_posix()
    cancelled_glob = (raw_runs_dir() / "*" / "*_mmtis_zugfahrten_ausgefallen.csv").as_posix()
    cancelled_src = (
        f"read_csv('{cancelled_glob}', all_varchar=true, header=true)"
        if any(raw_runs_dir().glob("*/*_mmtis_zugfahrten_ausgefallen.csv"))
        else "(SELECT NULL AS betriebstag, NULL AS zugnummer WHERE false)"
    )
    g = [d.as_posix() for d in gtfs_dirs]

    def gtfs(name: str) -> str:
        return "read_csv([" + ", ".join(f"'{d}/{name}.txt'" for d in g) + "], all_varchar=true, header=true)"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _at_runs AS
        SELECT DISTINCT zugnummer AS num, CAST(betriebstag AS DATE) AS service_day,
               {_ts('abfahrtzeit_soll')} AS t_a, {_ts('abfahrtzeit_ist')} AS a_a,
               {_ts('ankunftzeit_soll')} AS t_b, {_ts('ankunftzeit_ist')} AS a_b
        FROM read_csv('{runs_glob}', all_varchar=true, header=true)
        WHERE CAST(betriebstag AS DATE) BETWEEN DATE '{first}' AND DATE '{last}'""")
    con.execute("CREATE OR REPLACE TEMP TABLE _at_runs AS "
                "SELECT row_number() OVER (ORDER BY service_day, num, t_a, t_b, a_a, a_b) AS rid, * "
                "FROM _at_runs")

    # Trips active per service day, from calendar and calendar_dates.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _at_active AS
        WITH days AS (
            SELECT CAST(d AS DATE) AS d
            FROM generate_series(DATE '{first}', DATE '{last}', INTERVAL 1 DAY) g(d)
        ),
        cal AS (SELECT * FROM {gtfs('calendar')}),
        cd AS (SELECT service_id, CAST(strptime(date, '%Y%m%d') AS DATE) AS d, exception_type
               FROM {gtfs('calendar_dates')}),
        base AS (
            SELECT c.service_id, days.d FROM cal c JOIN days
              ON days.d BETWEEN CAST(strptime(c.start_date, '%Y%m%d') AS DATE)
                            AND CAST(strptime(c.end_date, '%Y%m%d') AS DATE)
             AND [c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday,
                  c.sunday][isodow(days.d)] = '1'
        )
        SELECT service_id, d FROM base ANTI JOIN (SELECT * FROM cd WHERE exception_type = '2')
          USING (service_id, d)
        UNION
        SELECT service_id, d FROM cd
        WHERE exception_type = '1' AND d BETWEEN DATE '{first}' AND DATE '{last}'""")

    # Local noon - 12 h of each service day, the GTFS time origin, as an epoch.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _at_trips AS
        WITH trips AS (SELECT * FROM {gtfs('trips')}),
             routes AS (SELECT * FROM {gtfs('routes')}),
             agency AS (SELECT * FROM {gtfs('agency')})
        SELECT t.trip_id, a.d AS service_day,
               regexp_extract(t.trip_short_name, '(\\d+)$', 1) AS num,
               nullif(regexp_extract(t.trip_short_name, '^([A-Za-z]+)', 1), '') AS prefix,
               t.trip_short_name AS label, ag.agency_name AS operator,
               CAST(epoch(timezone('{TZ}', CAST(a.d AS TIMESTAMP) + INTERVAL 12 HOUR)) AS BIGINT)
                 - 43200 AS t0
        FROM trips t
        JOIN _at_active a USING (service_id)
        JOIN routes r USING (route_id)
        LEFT JOIN agency ag ON ag.agency_id = r.agency_id""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _at_st AS
        SELECT trip_id, CAST(stop_sequence AS INTEGER) AS seq, stop_id,
               {_gtfs_secs('arrival_time')} AS arr_s, {_gtfs_secs('departure_time')} AS dep_s,
               coalesce(pickup_type, '0') = '1' AND coalesce(drop_off_type, '0') = '1' AS passing
        FROM {gtfs('stop_times')}""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _at_span AS
        WITH span AS (SELECT trip_id, min(dep_s) AS lo, max(arr_s) AS hi FROM _at_st GROUP BY 1)
        SELECT t.*, t.t0 + span.lo AS t_first, t.t0 + span.hi AS t_last
        FROM _at_trips t JOIN span USING (trip_id)""")

    # One-to-one: each run's best trip and each trip's best run, by scheduled overlap.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _at_pairs AS
        WITH cand AS (
            SELECT r.rid, t.trip_id, t.service_day,
                   least(t.t_last, coalesce(r.t_b, r.t_a))
                     - greatest(t.t_first, coalesce(r.t_a, r.t_b)) AS overlap
            FROM _at_runs r JOIN _at_span t
              ON t.num = r.num AND t.service_day = r.service_day
            WHERE coalesce(r.t_a, r.t_b) IS NOT NULL
        ),
        ranked AS (
            SELECT *, row_number() OVER (PARTITION BY rid ORDER BY overlap DESC, trip_id) AS kr,
                      row_number() OVER (PARTITION BY service_day, trip_id
                                         ORDER BY overlap DESC, rid) AS kt
            FROM cand WHERE overlap >= {MIN_OVERLAP_S}
        )
        SELECT rid, trip_id, service_day FROM ranked WHERE kr = 1 AND kt = 1""")

    with ingest._transaction(con):
        con.execute("DELETE FROM source_days WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute(f"""
            INSERT INTO source_days
            WITH ran AS (SELECT service_day, count(*) AS n FROM _at_runs GROUP BY 1),
                 cx AS (SELECT CAST(betriebstag AS DATE) AS service_day,
                               count(DISTINCT zugnummer) AS n
                        FROM {cancelled_src}
                        GROUP BY 1)
            SELECT ran.service_day, ran.n + coalesce(cx.n, 0), coalesce(cx.n, 0)
            FROM ran LEFT JOIN cx USING (service_day)""")
        con.execute("DELETE FROM stg_stops WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute(f"""
            INSERT INTO stg_stops
            WITH obs AS (
                SELECT p.trip_id, p.service_day, r.t_a, r.t_b,
                       CASE WHEN r.a_a - r.t_a BETWEEN {MIN_DELAY_S} AND {MAX_DELAY_S}
                            THEN r.a_a - r.t_a END AS d_a,
                       CASE WHEN r.a_b - r.t_b BETWEEN {MIN_DELAY_S} AND {MAX_DELAY_S}
                            THEN r.a_b - r.t_b END AS d_b
                FROM _at_pairs p JOIN _at_runs r USING (rid)
            ),
            obs_ok AS (SELECT * FROM obs WHERE d_a IS NOT NULL OR d_b IS NOT NULL),
            trip AS (
                SELECT t.*, o.t_a, o.d_a, o.t_b, o.d_b,
                       row_number() OVER (PARTITION BY t.service_day, t.label
                                          ORDER BY t.t_first, t.trip_id) AS k
                FROM _at_span t JOIN obs_ok o USING (trip_id, service_day)
            ),
            st AS (
                SELECT t.*, s.seq, s.stop_id,
                       row_number() OVER (PARTITION BY t.service_day, t.trip_id ORDER BY s.seq) AS pos,
                       count(*) OVER (PARTITION BY t.service_day, t.trip_id) AS n_stops,
                       t.t0 + s.arr_s AS s_arr, t.t0 + s.dep_s AS s_dep
                FROM trip t JOIN _at_st s USING (trip_id)
                WHERE NOT s.passing
            ),
            delayed AS (
                SELECT *,
                       {_delay_sql('s_arr')} AS dl_arr,
                       {_delay_sql('s_dep')} AS dl_dep
                FROM st
            )
            SELECT x.service_day,
                   CASE WHEN x.k = 1 THEN x.label ELSE x.label || ' #' || x.k END AS trip_id,
                   x.operator,
                   x.num                                          AS train_number,
                   {_category_sql('x.prefix')}                    AS category,
                   x.label                                        AS line,
                   coalesce(d.bpuic, -1)                          AS station_id,
                   x.pos                                          AS stop_seq,
                   CASE WHEN x.pos > 1 THEN x.s_arr END           AS sched_arr,
                   CASE WHEN x.pos < x.n_stops THEN x.s_dep END   AS sched_dep,
                   CASE WHEN x.pos > 1 THEN x.s_arr + x.dl_arr END AS act_arr,
                   CASE WHEN x.pos < x.n_stops
                        THEN greatest(x.s_dep + x.dl_dep,
                                      CASE WHEN x.pos > 1 THEN x.s_arr + x.dl_arr END) END
                                                                  AS act_dep,
                   x.pos > 1                                      AS arr_measured,
                   x.pos < x.n_stops                              AS dep_measured,
                   NULL                                           AS source_revision
            FROM delayed x
            LEFT JOIN '{dim_station_parquet.as_posix()}' d
              ON d.ifopt = {_ifopt_sql('x.stop_id')}""")

    runs, paired, trips = con.execute("""
        SELECT (SELECT count(*) FROM _at_runs), (SELECT count(*) FROM _at_pairs),
               (SELECT count(*) FROM _at_trips)""").fetchone()
    n_staged, n_unmatched, n_trips = con.execute(
        "SELECT count(*), count(*) FILTER (station_id < 0), count(DISTINCT (service_day, trip_id)) "
        "FROM stg_stops WHERE service_day BETWEEN ? AND ?", [first, last]
    ).fetchone()
    return {
        "runs": runs,
        "timetabled_trips": trips,
        "paired": paired,
        "runs_without_trip": runs - paired,
        "trips_without_run": trips - paired,
        "trips_published": n_trips,
        "stops_staged": n_staged,
        "stops_unknown_station": n_unmatched,
    }


def _delay_sql(s: str) -> str:
    """The delay at scheduled time s from the run's two observations (see module docstring)."""
    return f"""CAST(round(CASE
        WHEN d_a IS NULL OR t_a IS NULL THEN d_b
        WHEN d_b IS NULL OR t_b IS NULL THEN d_a
        WHEN {s} <= t_a OR t_b <= t_a THEN d_a
        WHEN {s} >= t_b THEN d_b
        ELSE d_a + (d_b - d_a) * ({s} - t_a) / (t_b - t_a)
    END) AS BIGINT)"""
