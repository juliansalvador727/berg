"""Great Britain adapter: an archive of the Darwin Push Port → normalized stop events.

Darwin is National Rail's national passenger-information system. Its Push Port stream carries
every passenger schedule (the daily timetable load plus every later change) and every train
status update with actual times from track circuits, TRUST and GPS. David Wheatley archives
the raw stream from the Rail Data Marketplace into hourly gzipped files, one XML document per
line, from 2025-09-07 on (see docs/data-notes-gb.md). Those files are the raw source; they
never go to R2.

Two stages:

  extract   raw hour files of one archive day → two small Parquet files: every schedule
            location, and every actual time. Each hour is parsed exactly once.
  stage     extracts of the service day, the day before (early timetable loads) and the day
            after (trains past midnight) → stg_stops.

Normalization rules:

  - A service's schedule is its LAST schedule message; each one replaces the whole service.
  - Passenger trains only: isPassengerSvc not false, status P or 1 (permanent / short-term
    passenger train), category not empty stock, bus or ship. Deleted schedules are dropped.
  - Public calls only: a location with a public arrival or departure time. Passing points and
    working-only stops are operational and never become stations.
  - A time is an observation only when Darwin sends it as `at` (actual). `et`/`wet` are
    forecasts and are never read. An actual Darwin later removed (atRemoved) is dropped.
  - A location the schedule marks cancelled keeps its row with no times, so the leg builder
    never bridges a part-cancelled train across the gap (stops.py's not_run verdict).
  - Times are local HH:MM[:SS] relative to the schedule start date (ssd). They are unwrapped
    past midnight along the schedule's working times, and each public or actual time is
    placed on the day nearest its location's working time.
  - Clock changes, as Darwin writes them: on the autumn night timetable times stay in the
    start day's BST throughout (a train timed 01:58 then 02:14 runs 16 minutes, not 76); on
    the spring night they are wall-clock (00:54 GMT then 02:02 BST is 8 minutes). Actual
    times are the wall clock when they happened, so on a clock-change night each is placed
    with whichever offset lands it nearer its scheduled instant.
"""

import gzip
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path

from berg_pipeline import ingest
from berg_pipeline.europe.config import GREAT_BRITAIN as CFG

ARCHIVE = (
    "https://ilovetrains.co.uk/api/download-darwin-dump?fileName={y}%2F{m}%2F{d}%2F{h}.pport.gz"
)
NAPTAN = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv"
USER_AGENT = "berg/europe-archive (historical train map; sequential downloads)"
# Rail Data Marketplace timetable status codes: P permanent and 1 short-term-planned passenger
# trains. B/5 bus, S/4 ship, F/2 freight and T/3 trip schedules are not trains a passenger rides.
PASSENGER_STATUS = ("P", "1")
# Darwin train categories that are not passenger trains even with a passenger status:
# EE/EL/ES empty coaching stock, BR/BS replacement and scheduled buses, SS ship.
NON_PASSENGER_CATS = ("EE", "EL", "ES", "BR", "BS", "SS")
# Operators in Darwin that are not the national railway: London Underground, Tyne and Wear
# Metro, Sheffield Supertram tram-trains and the North Yorkshire Moors heritage railway. Their
# stations are mostly absent from NaPTAN's rail list, so they would only half-render.
NON_NATIONAL_RAIL_TOCS = ("LT", "TW", "SJ", "NY")

SCHEDULE_COLS = (
    "ts rid uid train_id ssd toc train_cat status is_passenger deleted "
    "idx kind tpl act pta ptd wta wtd wtp can"
).split()
ACTUAL_COLS = "ts rid ssd tpl wta wtd wtp kind at removed".split()


def hour_path(day: date, hour: int) -> Path:
    return CFG.raw_dir / "pport" / f"{day:%Y/%m/%d}" / f"{hour:02d}.pport.gz"


def missing_marker(day: date, hour: int) -> Path:
    """Left in place of an hour the archive answers 404 for: a source gap, not a failure."""
    return hour_path(day, hour).with_suffix(".missing")


def extract_dir() -> Path:
    return CFG.root / "extract"


# ---------------------------------------------------------------------------------- fetch


def fetch_day(client, day: date) -> int:
    """Download the 24 hour files of one archive day, atomically each. An existing file or
    missing marker is trusted. Returns the bytes downloaded (0 if the day was complete)."""
    got = 0
    for hour in range(24):
        out = hour_path(day, hour)
        if out.exists() or missing_marker(day, hour).exists():
            continue
        url = ARCHIVE.format(y=f"{day:%Y}", m=f"{day:%m}", d=f"{day:%d}", h=f"{hour:02d}")
        body = _get(client, url)
        if body is None:
            out.parent.mkdir(parents=True, exist_ok=True)
            missing_marker(day, hour).touch()
            print(f"  {day} {hour:02d}:00 is not in the archive", file=sys.stderr, flush=True)
            continue
        # A truncated download fails here rather than as a short hour at build time.
        gzip.decompress(body)
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".tmp")
        tmp.write_bytes(body)
        tmp.rename(out)
        got += len(body)
    return got


def _get(client, url: str, attempts: int = 8) -> bytes | None:
    """The body, or None for a 404. Other 4xx raise; 429, 5xx and transport errors retry."""
    import httpx

    for attempt in range(attempts):
        try:
            r = client.get(url)
        except httpx.TransportError as e:
            if attempt == attempts - 1:
                raise
            reason = type(e).__name__
        else:
            if r.status_code == 200:
                return r.content
            if r.status_code == 404:
                return None
            if r.status_code != 429 and r.status_code < 500:
                r.raise_for_status()
            reason = f"HTTP {r.status_code}"
        delay = min(300, 5 * 2**attempt)
        print(f"  {url[-24:]}: {reason}, retry in {delay}s", file=sys.stderr, flush=True)
        time.sleep(delay)
    raise RuntimeError(f"{url}: giving up")


def fetch_stations(client) -> Path:
    out = CFG.raw_dir / "naptan_stops.csv"
    if not out.exists():
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".tmp")
        tmp.write_bytes(_get(client, NAPTAN))
        tmp.rename(out)
    return out


# -------------------------------------------------------------------------------- extract


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_hour(path: Path, schedules, actuals) -> None:
    """One hour file → rows passed to the schedules / actuals callables (*_COLS order)."""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if "<schedule " not in line and "<TS " not in line:
                continue
            root = ET.fromstring(line)
            ts = root.get("ts")
            for update in root:  # uR / sR
                for msg in update:
                    kind = _local(msg.tag)
                    if kind == "schedule":
                        _schedule(ts, msg, schedules)
                    elif kind == "TS":
                        _status(ts, msg, actuals)


def _schedule(ts: str, s, out) -> None:
    head = (
        ts,
        s.get("rid"),
        s.get("uid"),
        s.get("trainId"),
        s.get("ssd"),
        s.get("toc"),
        s.get("trainCat") or "OO",  # the schema's default
        s.get("status") or "P",
        s.get("isPassengerSvc") != "false",
        s.get("deleted") == "true",
    )
    idx = 0
    for loc in s:
        kind = _local(loc.tag)
        if kind not in ("OR", "OPOR", "IP", "OPIP", "PP", "DT", "OPDT"):
            continue  # cancelReason, lateReason
        g = loc.get
        out(
            head
            + (
                idx,
                kind,
                g("tpl"),
                g("act"),
                g("pta"),
                g("ptd"),
                g("wta"),
                g("wtd"),
                g("wtp"),
                g("can") == "true",
            )
        )
        idx += 1


def _status(ts: str, s, out) -> None:
    rid, ssd = s.get("rid"), s.get("ssd")
    for loc in s:
        if _local(loc.tag) != "Location":
            continue
        key = (loc.get("tpl"), loc.get("wta"), loc.get("wtd"), loc.get("wtp"))
        for ev in loc:
            kind = _local(ev.tag)
            if kind not in ("arr", "dep", "pass"):
                continue
            at, removed = ev.get("at"), ev.get("atRemoved") == "true"
            if at or removed:
                out((ts, rid, ssd) + key + (kind, at, removed))


def extract_day(day: date) -> dict:
    """An archive day's 24 hour files → extract/<day>.{schedules,actuals}.parquet.
    Idempotent; an existing pair is trusted."""
    import csv

    import duckdb

    sched_out = extract_dir() / f"{day}.schedules.parquet"
    act_out = extract_dir() / f"{day}.actuals.parquet"
    if sched_out.exists() and act_out.exists():
        return {"day": day.isoformat(), "cached": True}
    hours = [h for h in range(24) if hour_path(day, h).exists()]
    missing = [h for h in range(24) if h not in hours and not missing_marker(day, h).exists()]
    if missing:
        raise RuntimeError(f"{day}: hours not fetched: {missing}")
    extract_dir().mkdir(parents=True, exist_ok=True)
    sched_csv, act_csv = sched_out.with_suffix(".csv.gz"), act_out.with_suffix(".csv.gz")
    counts = [0, 0]
    with (
        gzip.open(sched_csv, "wt", newline="", compresslevel=1) as fs,
        gzip.open(act_csv, "wt", newline="", compresslevel=1) as fa,
    ):
        ws, wa = csv.writer(fs), csv.writer(fa)
        ws.writerow(SCHEDULE_COLS)
        wa.writerow(ACTUAL_COLS)

        def sched_row(r):
            counts[0] += 1
            ws.writerow(r)

        def act_row(r):
            counts[1] += 1
            wa.writerow(r)

        for hour in hours:
            parse_hour(hour_path(day, hour), sched_row, act_row)

    con = duckdb.connect()
    for tmp_csv, out in ((sched_csv, sched_out), (act_csv, act_out)):
        tmp = out.with_suffix(".tmp")
        con.execute(f"""
            COPY (SELECT * FROM read_csv('{tmp_csv.as_posix()}', header=true, all_varchar=true))
            TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)""")
        tmp.rename(out)
        tmp_csv.unlink()
    return {"day": day.isoformat(), "schedule_rows": counts[0], "actuals": counts[1]}


def gap_hours(first: date, last: date) -> list[str]:
    """UTC hours ('YYYY-MM-DDTHH') of [first, last] whose archive file is missing. The file is
    named by local hour, so an hour repeated when the clocks go back is two UTC hours."""
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(CFG.timezone)
    out = set()
    for day in (first + timedelta(days=i) for i in range((last - first).days + 1)):
        for hour in range(24):
            if missing_marker(day, hour).exists():
                for fold in (0, 1):
                    local = datetime(day.year, day.month, day.day, hour, fold=fold, tzinfo=tz)
                    u = local.astimezone(timezone.utc)
                    # The hour the clocks skip in spring has no file to miss.
                    if u.astimezone(tz).replace(tzinfo=None) == local.replace(tzinfo=None):
                        out.add(u.strftime("%Y-%m-%dT%H"))
    return sorted(out)


# Local hours whose files carry the day's timetable load (28k + 17k schedule messages on a
# weekday, a few hundred in every other hour).
TIMETABLE_LOAD_HOURS = (2, 3)


def lost_timetable_days(first: date, last: date) -> list[date]:
    """Service days whose timetable load is missing from the archive. They cannot be
    rebuilt from the day's later changes, so they are published as missing coverage."""
    days = (first + timedelta(days=i) for i in range((last - first).days + 1))
    return [d for d in days if any(missing_marker(d, h).exists() for h in TIMETABLE_LOAD_HOURS)]


# ------------------------------------------------------------------------------- stations

_B37 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def station_id(tiploc: str) -> int:
    """TIPLOC → dataset-local id: the code read as a base-37 number (digit 0 is 'no char').
    Deterministic, reversible, and under 2**53 for Darwin's seven-character codes."""
    n = 0
    for c in tiploc.upper().ljust(7)[:7]:
        n = n * 37 + (0 if c == " " else _B37.index(c) + 1)
    return n


def _station_id_sql(expr: str) -> str:
    """station_id in SQL; the two must agree (tested)."""
    parts = [
        f"coalesce(nullif(instr('{_B37}', substr(rpad(upper({expr}), 7, ' '), {i + 1}, 1)), 0), 0)"
        f" * {37 ** (6 - i)}"
        for i in range(7)
    ]
    return "CAST(" + " + ".join(parts) + " AS BIGINT)"


# Darwin TIPLOCs for a public call that NaPTAN files under another TIPLOC of the same station.
TIPLOC_ALIASES = {
    "HIGHBYE": "HIGHBYA",  # Highbury & Islington, East London line platforms
    "CNNBELL": "CNNB",  # Canonbury, East London line
    "WORCPHL": "WORCPWY",  # Worcestershire Parkway high level
    "ABDARAR": "ABDARE",
    "HEYSHBR": "HEYMST",  # Heysham Port
}


# NaPTAN points that sit too far from the track to snap (the geometry job's ceiling is 300 m),
# moved onto the platform line measured from OpenStreetMap.
COORDINATE_OVERRIDES = {
    "GLAZBRK": (-2.45525, 53.42940),  # NaPTAN is 315 m west, on the station approach road
}


def write_dim_station(naptan_csv: Path, out: Path) -> dict:
    """NaPTAN rail stations → dim_station. A NaPTAN rail access node's ATCO code is '9100'
    followed by the station's TIPLOC, which is exactly what Darwin schedules call at.

    Inactive rows are kept: Darwin still calls at superseded TIPLOCs such as STPANCI. A few
    recent rows (the Elizabeth line core) carry only an OS grid reference; they are placed
    from the nearest station with both, by the grid offset in metres.
    """
    import duckdb

    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE n AS
        SELECT substr(ATCOCode, 5) AS tiploc,
               regexp_replace(CommonName, ' Rail Station$', '') AS name,
               CAST(Easting AS DOUBLE) AS e, CAST(Northing AS DOUBLE) AS n,
               TRY_CAST(Longitude AS DOUBLE) AS lon, TRY_CAST(Latitude AS DOUBLE) AS lat
        FROM read_csv('{naptan_csv.as_posix()}', header=true, all_varchar=true)
        WHERE StopType = 'RLY' AND ATCOCode LIKE '9100%'""")
    con.execute("""
        CREATE TABLE s AS
        SELECT tiploc, name, lon, lat FROM n WHERE lon IS NOT NULL
        UNION ALL
        SELECT m.tiploc, m.name,
               arg_min(r.lon + (m.e - r.e) / (111320 * cos(radians(r.lat))),
                       (m.e - r.e) ^ 2 + (m.n - r.n) ^ 2),
               arg_min(r.lat + (m.n - r.n) / 111320, (m.e - r.e) ^ 2 + (m.n - r.n) ^ 2)
        FROM n m, n r
        WHERE m.lon IS NULL AND r.lon IS NOT NULL
        GROUP BY m.tiploc, m.name""")
    con.executemany(
        "INSERT INTO s SELECT ?, name, lon, lat FROM s WHERE tiploc = ?",
        list(TIPLOC_ALIASES.items()),
    )
    con.executemany(
        "UPDATE s SET lon = ?, lat = ? WHERE tiploc = ?",
        [(lon, lat, t) for t, (lon, lat) in COORDINATE_OVERRIDES.items()],
    )
    n, ids = con.execute("SELECT count(*), count(DISTINCT tiploc) FROM s").fetchone()
    if n != ids:
        raise RuntimeError(f"duplicate TIPLOCs in NaPTAN: {n} rows, {ids} codes")
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (SELECT {_station_id_sql("tiploc")} AS bpuic, name, tiploc AS code, lon, lat,
                     true AS passenger, 'GB' AS country,
                     DATE '1900-01-01' AS valid_from, DATE '9999-12-31' AS valid_to
              FROM s ORDER BY bpuic)
        TO '{out.as_posix()}' (FORMAT PARQUET)""")
    return {"stations": n}


# ---------------------------------------------------------------------------------- stage


def _secs(expr: str) -> str:
    """'HH:MM' or 'HH:MM:SS' → seconds of day."""
    return (
        f"(CAST(split_part({expr}, ':', 1) AS INTEGER) * 3600"
        f" + CAST(split_part({expr}, ':', 2) AS INTEGER) * 60"
        f" + coalesce(TRY_CAST(nullif(split_part({expr}, ':', 3), '') AS INTEGER), 0))"
    )


def _near(t: str, ref: str) -> str:
    """Seconds of day t placed on the day that brings it closest to unwrapped time ref."""
    return f"({t} + 86400 * round(({ref} - {t}) / 86400.0))"


def _offset(day: str) -> str:
    """Europe/London's UTC offset in seconds at noon of a date."""
    noon = f"(CAST({day} AS TIMESTAMP) + INTERVAL 12 HOUR)"
    return f"CAST(epoch({noon}) - epoch(timezone('{CFG.timezone}', {noon})) AS BIGINT)"


def _scheduled(t: str) -> str:
    """A timetable time's unwrapped local seconds → epoch UTC. Darwin writes the night the
    clocks go back in the start day's BST throughout (01:58 then 02:14 is 16 minutes), and
    the night they go forward in wall-clock time (00:54 GMT then 02:02 BST is 8 minutes)."""
    wall = f"epoch(timezone('{CFG.timezone}', CAST(ssd AS TIMESTAMP) + to_seconds({t})))"
    return f"CAST(CASE WHEN off1 < off0 THEN base + {t} - off0 ELSE {wall} END AS BIGINT)"


def _actual(t: str, ref: str) -> str:
    """An actual's wall-clock seconds → epoch UTC. On a clock-change night the wall clock the
    signaller read may be either side of the change, so the offset that lands the event nearer
    its scheduled instant wins; any other night has one offset."""
    a0, a1 = f"(base + {t} - off0)", f"(base + {t} - off1)"
    return (
        f"CAST(CASE WHEN off0 = off1 OR abs({a0} - ({ref})) <= abs({a1} - ({ref})) "
        f"THEN {a0} ELSE {a1} END AS BIGINT)"
    )


def stage_days(con, days: list[date]) -> dict:
    """Extracts → stg_stops and source_days for exactly these service days. Idempotent."""
    from berg_pipeline.europe import stops

    stops.create_tables(con)
    first, last = min(days), max(days)
    archive = [
        first - timedelta(days=1) + timedelta(days=i) for i in range((last - first).days + 3)
    ]
    have = [d for d in archive if (extract_dir() / f"{d}.schedules.parquet").exists()]
    for d in days:
        if d not in have:
            raise RuntimeError(f"{d}: archive day not extracted")
    sched = (
        "["
        + ", ".join(f"'{(extract_dir() / f'{d}.schedules.parquet').as_posix()}'" for d in have)
        + "]"
    )
    acts = (
        "["
        + ", ".join(f"'{(extract_dir() / f'{d}.actuals.parquet').as_posix()}'" for d in have)
        + "]"
    )
    cats = ", ".join(f"'{c}'" for c in NON_PASSENGER_CATS)
    statuses = ", ".join(f"'{c}'" for c in PASSENGER_STATUS)
    tocs = ", ".join(f"'{c}'" for c in NON_NATIONAL_RAIL_TOCS)

    # The latest schedule message per service; ts is ISO-8601 with offset, so it sorts once
    # parsed. A service is in scope by the schedule's own start date.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _gb_sched AS
        WITH s AS (
            SELECT * FROM read_parquet({sched})
            WHERE CAST(ssd AS DATE) BETWEEN DATE '{first}' AND DATE '{last}'
        ),
        latest AS (
            SELECT rid, max(CAST(ts AS TIMESTAMPTZ)) AS ts FROM s GROUP BY rid
        )
        SELECT s.* EXCLUDE (ts, idx), CAST(idx AS INTEGER) AS idx
        FROM s JOIN latest l ON s.rid = l.rid AND CAST(s.ts AS TIMESTAMPTZ) = l.ts""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _gb_services AS
        SELECT rid, any_value(CAST(ssd AS DATE)) AS ssd, any_value(uid) AS uid,
               any_value(train_id) AS train_id, any_value(toc) AS toc,
               any_value(train_cat) AS train_cat,
               bool_and(deleted = 'True') AS deleted,
               any_value(is_passenger = 'True' AND status IN ({statuses})
                         AND train_cat NOT IN ({cats})
                         AND toc NOT IN ({tocs})) AS passenger,
               count(*) FILTER (pta IS NOT NULL OR ptd IS NOT NULL) AS public_calls,
               count(*) FILTER ((pta IS NOT NULL OR ptd IS NOT NULL) AND can = 'True')
                   AS cancelled_calls
        FROM _gb_sched GROUP BY rid""")

    # Unwrap working times past midnight: each location's first working time, then a day is
    # added whenever the clock falls more than 12 h behind the previous location.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _gb_calls AS
        WITH loc AS (
            SELECT s.*, {_secs("coalesce(wta, wtd, wtp)")} AS w0
            FROM _gb_sched s JOIN _gb_services v USING (rid)
            WHERE v.passenger AND NOT v.deleted
        ),
        step AS (
            SELECT *, CASE WHEN w0 < lag(w0) OVER (PARTITION BY rid ORDER BY idx) - 43200
                           THEN 1 ELSE 0 END AS wrapped
            FROM loc
        ),
        wrap AS (
            SELECT *, sum(wrapped) OVER (PARTITION BY rid ORDER BY idx) AS days_on FROM step
        )
        SELECT rid, ssd, idx, tpl, wta, wtd, wtp, can = 'True' AS cancelled,
               w0 + 86400 * coalesce(days_on, 0) AS w,
               CASE WHEN pta IS NOT NULL THEN {_near(_secs("pta"), "w0 + 86400 * coalesce(days_on, 0)")} END AS pta_s,
               CASE WHEN ptd IS NOT NULL THEN {_near(_secs("ptd"), "w0 + 86400 * coalesce(days_on, 0)")} END AS ptd_s
        FROM wrap
        WHERE pta IS NOT NULL OR ptd IS NOT NULL""")

    # Actuals match a call by (rid, tpl, working times) — Darwin's own key for a location, so
    # a train calling twice at one station keeps its two visits apart. The latest message per
    # side wins; if that one removed the actual, the side has none.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _gb_act AS
        WITH a AS (
            SELECT rid, tpl, wta, wtd, wtp, kind, "at", removed = 'True' AS removed,
                   row_number() OVER (PARTITION BY rid, tpl, wta, wtd, wtp, kind
                                      ORDER BY CAST(ts AS TIMESTAMPTZ) DESC) AS rn
            FROM read_parquet({acts})
            WHERE kind IN ('arr', 'dep')
              AND CAST(ssd AS DATE) BETWEEN DATE '{first}' AND DATE '{last}'
        )
        SELECT rid, tpl, wta, wtd, wtp,
               any_value("at") FILTER (kind = 'arr') AS at_arr,
               any_value("at") FILTER (kind = 'dep') AS at_dep
        FROM a WHERE rn = 1 AND NOT removed AND "at" IS NOT NULL
        GROUP BY ALL""")

    with ingest._transaction(con):
        con.execute("DELETE FROM source_days WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute("""
            INSERT INTO source_days
            SELECT ssd, count(*),
                   count(*) FILTER (public_calls > 0 AND cancelled_calls = public_calls)
            FROM _gb_services WHERE passenger AND NOT deleted AND public_calls > 0
            GROUP BY ssd""")
        con.execute("DELETE FROM stg_stops WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute(f"""
            INSERT INTO stg_stops
            WITH j AS (
                SELECT c.*, v.toc, v.train_id,
                       CASE WHEN NOT c.cancelled AND a.at_arr IS NOT NULL
                            THEN {_near(_secs("a.at_arr"), "coalesce(c.pta_s, c.w)")} END AS aa,
                       CASE WHEN NOT c.cancelled AND a.at_dep IS NOT NULL
                            THEN {_near(_secs("a.at_dep"), "coalesce(c.ptd_s, c.w)")} END AS ad
                FROM _gb_calls c
                JOIN _gb_services v USING (rid)
                LEFT JOIN _gb_act a
                  ON a.rid = c.rid AND a.tpl = c.tpl
                 AND a.wta IS NOT DISTINCT FROM c.wta AND a.wtd IS NOT DISTINCT FROM c.wtd
                 AND a.wtp IS NOT DISTINCT FROM c.wtp
            )
            ,
            k AS (
                SELECT *, epoch(CAST(ssd AS TIMESTAMP)) AS base,
                       {_offset("ssd")} AS off0, {_offset("CAST(ssd AS DATE) + 1")} AS off1
                FROM j
            )
            ,
            r AS (
                SELECT ssd, rid, toc, train_id, idx, tpl,
                       CASE WHEN NOT cancelled AND pta_s IS NOT NULL
                            THEN {_scheduled("pta_s")} END AS sa,
                       CASE WHEN NOT cancelled AND ptd_s IS NOT NULL
                            THEN {_scheduled("ptd_s")} END AS sd,
                       CASE WHEN aa IS NOT NULL
                            THEN {_actual("aa", _scheduled("coalesce(pta_s, w)"))} END AS xa,
                       CASE WHEN ad IS NOT NULL
                            THEN {_actual("ad", _scheduled("coalesce(ptd_s, w)"))} END AS xd
                FROM k
            ),
            -- Two consecutive calls at one TIPLOC (a train reversing or dividing) are one
            -- stop: the first call's arrival and the second's departure.
            m AS (
                SELECT *, lag(tpl) OVER w = tpl AS dup_prev, lead(tpl) OVER w = tpl AS dup_next,
                       lead(sd) OVER w AS next_sd, lead(xd) OVER w AS next_xd
                FROM r WINDOW w AS (PARTITION BY rid ORDER BY idx)
            ),
            one AS (
                SELECT ssd, rid, toc, train_id, idx, tpl, sa, xa,
                       CASE WHEN dup_next THEN next_sd ELSE sd END AS sd,
                       CASE WHEN dup_next THEN next_xd ELSE xd END AS xd
                FROM m WHERE NOT coalesce(dup_prev, false)
            )
            SELECT ssd, rid, toc, train_id, toc, toc || ' ' || train_id,
                   {_station_id_sql("tpl")},
                   row_number() OVER (PARTITION BY rid ORDER BY idx),
                   sa, sd, xa, xd, xa IS NOT NULL, xd IS NOT NULL, NULL
            FROM one""")

    s = con.execute("""
        SELECT count(*), count(*) FILTER (passenger AND NOT deleted AND public_calls > 0),
               count(*) FILTER (passenger AND NOT deleted AND public_calls > 0
                                AND cancelled_calls = public_calls),
               count(*) FILTER (passenger AND NOT deleted AND cancelled_calls > 0
                                AND cancelled_calls < public_calls)
        FROM _gb_services""").fetchone()
    c = con.execute(f"""
        SELECT count(*), count(act_arr), count(act_dep),
               count(*) FILTER (sched_arr IS NULL AND sched_dep IS NULL)
        FROM stg_stops WHERE service_day BETWEEN DATE '{first}' AND DATE '{last}'""").fetchone()
    return {
        "services": s[0],
        "passenger_services": s[1],
        "cancelled_services": s[2],
        "part_cancelled_services": s[3],
        "stops_staged": c[0],
        "observed_arrivals": c[1],
        "observed_departures": c[2],
        "cancelled_calls": c[3],
    }
