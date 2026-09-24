"""Germany adapter: DB Timetables API (IRIS) crawls → normalized stop events.

The source is piebro/deutsche-bahn-data on Hugging Face, which crawls DB's Timetables API
for every station every six hours and publishes one parquet file per month: one row per stop
of every ride, with scheduled and last-changed arrival/departure times in Berlin local time
(see docs/data-notes-de.md). Files are fetched at a pinned revision and checked against the
repository's SHA-256, because the maintainer reprocesses history when the schema changes.
The raw files stay under data/datasets/de/raw and never go to R2.

Normalization rules:

  - A ride is the IRIS stop id without its last field: `<trip hash>-<yymmddHHMM>-<stop>`.
    The hash repeats every day, the middle field is the ride's first scheduled departure
    (its service day), and the last is the stop's position on the ride. Stop order is that
    position, never time.
  - Trains only. Road replacement (train type Bus/SEV/Taxi, or an SEV/EV line under a rail
    operator's code) is dropped.
  - Stops added by a diversion are appended at positions after the regular route and carry
    garbage scheduled times, so they are dropped and counted.
  - A cancelled arrival or departure nulls that side, and a stop with neither side left is
    dropped. Consecutive kept stops whose positions are not adjacent are never bridged: the
    gap is a cancelled stop, a foreign stop IRIS does not list, or an hour the crawler
    missed, and the stop builder must not draw a leg across it.
  - A change time is the last one the crawler saw. With no realtime message it equals the
    scheduled time, so "on time" and "never reported" look the same: the dataset's
    semantics are final_prediction, and it is never labelled observed.
"""

import hashlib
import json
import os
import time
from datetime import date, timedelta
from pathlib import Path

from berg_pipeline import ingest
from berg_pipeline.europe.config import GERMANY

REPO = "piebro/deutsche-bahn-data"
# Pinned 2026-09-24. The maintainer rewrites historical months when the schema changes
# (2026-05, 2026-08, 2026-09), so every build reads one fixed revision.
REVISION = "2a161c0b9957cd58297f841c818bb8cbbdaf2798"
FILE_URL = f"https://huggingface.co/datasets/{REPO}/resolve/{REVISION}/{{path}}"
TREE_URL = f"https://huggingface.co/api/datasets/{REPO}/tree/{REVISION}/{{path}}"

REQUIRED_COLUMNS = frozenset({
    "eva", "train_number", "line_number", "train_type", "id",
    "arrival_planned_time", "arrival_change_time", "departure_planned_time",
    "departure_change_time", "arrival_is_canceled", "departure_is_canceled",
    "is_additional_stop",
})

# IRIS's train category is a product (ICE, RE, S) for DB and many others, and an operator
# code (ag, erx, VIA, HLB) for the rest. Products are kept as they are.
PRODUCTS = (
    "ICE", "IC", "EC", "ECE", "RJ", "RJX", "TGV", "EST", "FLX", "WB", "NJ", "EN", "ES", "D",
    "IR", "IRE", "RE", "RB", "S", "MEX", "RS", "FEX",
    # Regional products of the Austrian and Czech trains that reach German stations.
    "R", "OS",
)
# An operator code takes its product from the line number's prefix ("RB23", "S5"); any
# other line under an operator code is a regional train.
LINE_PRODUCTS = {"S": "S", "RE": "RE", "RB": "RB", "IRE": "IRE", "MEX": "MEX", "RS": "RS",
                 "FEX": "FEX", "HBX": "RE", "REX": "RE"}
# Road replacement, matched case-insensitively on the train type or the line's prefix.
ROAD_TYPES = ("BUS", "SEV", "BSV", "BEV", "BBUS", "BS", "TAXI", "EV")
ROAD_LINES = ("SEV", "EV", "EVS", "SV", "EVRT", "BUS")

# A UTC hour with fewer scheduled stops than this share of the median for the same local
# hour of the week is a crawl hole. Holidays run a Sunday timetable and bottom out at ~35%
# of the weekday median in their first hours, so 25% separates them from real holes, which
# sit under 5%.
GAP_HOUR_SHARE = 0.25


def raw_path(month: str) -> Path:
    return GERMANY.raw_dir / f"data-{month}.parquet"


def stations_path(month: str) -> Path:
    return GERMANY.raw_dir / f"stada-{month}.json"


def _get(url: str):
    import httpx

    for attempt in range(1, 7):
        try:
            response = httpx.get(url, timeout=120, follow_redirects=True)
            response.raise_for_status()
            return response
        except Exception:
            if attempt == 6:
                raise
            time.sleep(min(60, 2**attempt))
    raise AssertionError("unreachable")


def _tree(path: str) -> list[dict]:
    return _get(TREE_URL.format(path=path)).json()


def _download(path: str, out: Path, sha256: str | None = None) -> None:
    """A repository file → out atomically, verified against its LFS SHA-256 when given."""
    import httpx

    tmp = out.with_name(f".{out.name}.tmp")
    out.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, 7):
        try:
            digest = hashlib.sha256()
            with httpx.stream("GET", FILE_URL.format(path=path), timeout=300,
                              follow_redirects=True) as response:
                response.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in response.iter_bytes(1 << 20):
                        digest.update(chunk)
                        fh.write(chunk)
            if sha256 and digest.hexdigest() != sha256:
                raise RuntimeError(f"{path}: sha256 {digest.hexdigest()} != {sha256}")
            os.replace(tmp, out)
            return
        except Exception:
            if attempt == 6:
                raise
            time.sleep(min(60, 2**attempt))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 24):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_month(month: str, force: bool = False) -> Path:
    """One month of stops at the pinned revision, SHA-256 checked either way."""
    out = raw_path(month)
    listing = {f["path"]: f for f in _tree("monthly_processed_data")}
    entry = listing.get(f"monthly_processed_data/data-{month}.parquet")
    if entry is None:
        raise RuntimeError(f"{month}: no monthly file at revision {REVISION[:12]}")
    sha = entry["lfs"]["oid"]
    if force or not out.exists() or _sha256(out) != sha:
        _download(entry["path"], out, sha)
    _check_columns(out)
    return out


def _check_columns(path: Path) -> None:
    import duckdb

    columns = {r[0] for r in duckdb.sql(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()}
    if missing := REQUIRED_COLUMNS - columns:
        raise RuntimeError(f"{path.name}: lacks {sorted(missing)}")


def fetch_stations(month: str, force: bool = False) -> Path:
    """DB's station list (StaDa) as the crawler saw it early in `month`.

    The crawl stores StaDa's seven category responses beside the timetable queries in its
    raw files. One snapshot per month is kept, as a compact JSON, so a station opened or
    closed inside the window is still found; the 30-50 MB raw file is discarded.
    """
    out = stations_path(month)
    if out.exists() and not force:
        return out
    import duckdb

    year, mon = month.split("-")
    first_day = 3 if month == "2025-11" else 1  # full-station crawling starts 2025-11-02
    for day in range(first_day, first_day + 5):
        files = sorted(_tree(f"raw_data/year={year}/month={int(mon)}/day={day}"),
                       key=lambda f: f["size"])
        for entry in files:
            tmp = GERMANY.raw_dir / f".stada-{month}.parquet"
            _download(entry["path"], tmp, entry.get("lfs", {}).get("oid"))
            try:
                responses = duckdb.sql(
                    f"SELECT url, response_data FROM '{tmp.as_posix()}' "
                    "WHERE api_name = 'station-data/v2/stations' AND status_code = '200'"
                ).fetchall()
            finally:
                tmp.unlink()
            stations = _parse_stada(responses)
            if len(responses) == 7 and stations:
                out.write_text(json.dumps(
                    {"source": entry["path"], "revision": REVISION, "stations": stations},
                    ensure_ascii=False))
                return out
    raise RuntimeError(f"{month}: no complete StaDa snapshot in the first raw days")


def _parse_stada(responses: list[tuple[str, str]]) -> list[dict]:
    """StaDa category responses → one row per EVA number (a station can have several)."""
    out = []
    for _url, body in responses:
        for station in json.loads(body)["result"]:
            ril = station.get("ril100Identifiers") or [{}]
            for eva in station.get("evaNumbers", []):
                coords = (eva.get("geographicCoordinates") or {}).get("coordinates")
                if not coords:
                    continue
                out.append({"eva": int(eva["number"]), "name": station["name"],
                            "code": ril[0].get("rilIdentifier"),
                            "lon": coords[0], "lat": coords[1]})
    return out


def write_dim_station(snapshots: list[Path], out: Path) -> dict:
    """StaDa snapshots → dim_station (bpuic = EVA number, the local id).

    A station in several snapshots takes its latest name and position. There is no validity
    history, so every row is valid for all time. StaDa lists only DB's German stations, so
    every one is DE; a foreign stop IRIS mentions is unmatched and never published.
    """
    import duckdb

    by_eva: dict[int, dict] = {}
    for path in sorted(snapshots):  # stada-YYYY-MM.json: oldest first, latest wins
        for row in json.loads(path.read_text())["stations"]:
            by_eva[row["eva"]] = row
    out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("CREATE TABLE s (bpuic BIGINT, name VARCHAR, code VARCHAR, lon DOUBLE, "
                "lat DOUBLE, passenger BOOLEAN, country VARCHAR)")
    con.executemany(
        "INSERT INTO s VALUES (?, ?, ?, ?, ?, true, 'DE')",
        [[r["eva"], r["name"], r["code"], r["lon"], r["lat"]] for r in by_eva.values()],
    )
    con.execute(f"""
        COPY (SELECT *, DATE '1900-01-01' AS valid_from, DATE '9999-12-31' AS valid_to
              FROM s ORDER BY bpuic)
        TO '{out.as_posix()}' (FORMAT PARQUET)""")
    return {"stations": len(by_eva), "snapshots": len(snapshots)}


def _utc(expr: str) -> str:
    """Naive Berlin local timestamp → epoch seconds UTC. The window has no autumn change,
    and nothing is scheduled in the missing spring hour."""
    return (f"CAST(epoch(timezone('{GERMANY.timezone}', CAST({expr} AS TIMESTAMP))) "
            "AS BIGINT)")


def _category_sql(train_type: str, line: str) -> str:
    products = ", ".join(f"'{p}'" for p in PRODUCTS)
    prefix = f"upper(regexp_extract({line}, '^([A-Za-z]+)', 1))"
    cases = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in LINE_PRODUCTS.items())
    return (f"CASE WHEN upper({train_type}) IN ({products}) THEN upper({train_type}) "
            f"WHEN upper({train_type}) = 'REX' THEN 'RE' "
            f"WHEN {prefix} IN ({', '.join(repr(k) for k in LINE_PRODUCTS)}) "
            f"THEN CASE {prefix} {cases} END "
            f"WHEN nullif(trim({line}), '') IS NOT NULL THEN 'RB' "
            f"ELSE coalesce(nullif(upper(trim({train_type})), ''), 'TRAIN') END")


def _files_for(first: date, last: date) -> list[Path]:
    """Monthly files holding service days [first, last]. A ride's stops are filed by the
    month of their own times, so the months either side are read when present: a ride of
    the 31st that ends after midnight has its last stops in the next file."""
    months = sorted({f"{d:%Y-%m}" for d in (first, last)})
    edge = sorted({f"{first - timedelta(days=1):%Y-%m}", f"{last + timedelta(days=2):%Y-%m}"})
    missing = [m for m in months if not raw_path(m).exists()]
    if missing:
        raise RuntimeError(f"raw months not fetched: {missing}")
    return [raw_path(m) for m in sorted(set(months) | set(edge)) if raw_path(m).exists()]


def stage_days(con, days: list[date], dim_station_parquet: Path) -> dict:
    """Monthly parquet → stg_stops for exactly these service days. Idempotent per day."""
    from berg_pipeline.europe import stops

    stops.create_tables(con)
    first, last = min(days), max(days)
    listed = "[" + ", ".join(f"'{f.as_posix()}'" for f in _files_for(first, last)) + "]"
    road_types = ", ".join(f"'{t}'" for t in ROAD_TYPES)
    road_lines = ", ".join(f"'{t}'" for t in ROAD_LINES)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _de_rows AS
        WITH src AS (
            SELECT *, regexp_extract(id, '^(-?[0-9]+)-([0-9]{{10}})-([0-9]+)$',
                                     ['hash', 'start', 'pos']) AS p
            FROM read_parquet({listed})
        )
        SELECT CAST(strptime(p.start, '%y%m%d%H%M') AS DATE)  AS service_day,
               p.hash || '-' || p.start                       AS ride,
               CAST(p.pos AS INTEGER)                         AS pos,
               CAST(eva AS BIGINT)                            AS eva,
               train_type, train_number, line_number,
               {_utc('arrival_planned_time')}                 AS s_arr,
               {_utc('arrival_change_time')}                  AS c_arr,
               {_utc('departure_planned_time')}               AS s_dep,
               {_utc('departure_change_time')}                AS c_dep,
               coalesce(arrival_is_canceled, false)           AS arr_cancelled,
               coalesce(departure_is_canceled, false)         AS dep_cancelled,
               coalesce(is_additional_stop, false)            AS additional,
               upper(coalesce(train_type, '')) IN ({road_types})
                 OR upper(regexp_extract(coalesce(line_number, ''), '^([A-Za-z]+)', 1))
                    IN ({road_lines})                         AS road
        FROM src
        WHERE p.start <> ''
          AND CAST(strptime(p.start, '%y%m%d%H%M') AS DATE)
              BETWEEN DATE '{first}' AND DATE '{last}'""")
    # Unparseable ids would silently drop stops, so they are counted over the same files.
    unparsed = con.execute(
        f"SELECT count(*) FROM read_parquet({listed}) "
        "WHERE NOT regexp_matches(id, '^-?[0-9]+-[0-9]{10}-[0-9]+$')"
    ).fetchone()[0]

    # A ride whose scheduled times run backwards along its positions is garbled; excluded
    # whole and counted, never reordered.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _de_malformed AS
        WITH o AS (
            SELECT ride, s_arr,
                   lag(coalesce(s_dep, s_arr)) OVER (PARTITION BY ride ORDER BY pos) AS prev
            FROM _de_rows WHERE NOT road AND NOT additional
        )
        SELECT ride FROM o GROUP BY ride HAVING count(*) FILTER (s_arr < prev) > 0""")

    with ingest._transaction(con):
        # What the source says about each day, for the thin-day check: a ride is cancelled
        # when no stop keeps a side.
        con.execute("DELETE FROM source_days WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute("""
            INSERT INTO source_days
            SELECT service_day, count(*), count(*) FILTER (cancelled)
            FROM (SELECT ride, any_value(service_day) AS service_day,
                         bool_and((s_arr IS NULL OR arr_cancelled)
                                  AND (s_dep IS NULL OR dep_cancelled)) AS cancelled
                  FROM _de_rows WHERE NOT road AND NOT additional GROUP BY ride)
            GROUP BY service_day""")
        con.execute("DELETE FROM stg_stops WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute(f"""
            INSERT INTO stg_stops
            WITH kept AS (
                SELECT r.*,
                       r.s_arr IS NOT NULL AND NOT r.arr_cancelled AS arr_live,
                       r.s_dep IS NOT NULL AND NOT r.dep_cancelled AS dep_live
                FROM _de_rows r
                ANTI JOIN _de_malformed USING (ride)
                WHERE NOT r.road AND NOT r.additional
            ),
            live AS (
                SELECT *,
                       -- The next kept stop is not the next position: never bridged.
                       lead(pos) OVER (PARTITION BY ride ORDER BY pos) - pos > 1 AS gap_after
                FROM kept WHERE arr_live OR dep_live
            ),
            ride AS (
                SELECT ride, any_value(service_day) AS service_day,
                       any_value(train_type) AS train_type,
                       any_value(train_number) AS number,
                       arg_min(line_number, pos) AS line,
                       {_category_sql('any_value(train_type)', 'arg_min(line_number, pos)')}
                           AS category,
                       min(coalesce(s_dep, s_arr)) AS t0
                FROM live GROUP BY ride
            ),
            labelled AS (
                SELECT *, category || ' ' || number AS label,
                       row_number() OVER (PARTITION BY service_day, category || ' ' || number
                                          ORDER BY t0, ride) AS k
                FROM ride
            )
            SELECT l.service_day,
                   -- S-Bahn networks reuse numbers (S 42183 runs in Berlin and Hamburg),
                   -- so a second ride with one label that day gets its own id.
                   CASE WHEN l.k = 1 THEN l.label ELSE l.label || ' #' || l.k END AS trip_id,
                   l.train_type                                   AS operator,
                   l.number                                       AS train_number,
                   l.category,
                   coalesce(l.line, l.label)                      AS line,
                   r.eva                                          AS station_id,
                   r.pos                                          AS stop_seq,
                   CASE WHEN r.arr_live THEN r.s_arr END           AS sched_arr,
                   CASE WHEN r.dep_live AND NOT r.gap_after IS TRUE THEN r.s_dep END
                                                                   AS sched_dep,
                   CASE WHEN r.arr_live THEN r.c_arr END           AS act_arr,
                   CASE WHEN r.dep_live AND NOT r.gap_after IS TRUE THEN r.c_dep END
                                                                   AS act_dep,
                   r.arr_live AND r.c_arr IS NOT NULL              AS arr_measured,
                   r.dep_live AND NOT r.gap_after IS TRUE AND r.c_dep IS NOT NULL
                                                                   AS dep_measured,
                   NULL                                            AS source_revision
            FROM live r
            JOIN labelled l USING (ride)""")

    stats = con.execute("""
        SELECT (SELECT count(DISTINCT ride) FROM _de_rows WHERE NOT road),
               (SELECT count(DISTINCT ride) FROM _de_rows WHERE road),
               (SELECT count(*) FROM _de_rows WHERE additional AND NOT road),
               (SELECT count(*) FROM _de_malformed),
               (SELECT count(*) FROM _de_rows WHERE NOT road AND (arr_cancelled OR dep_cancelled))
        """).fetchone()
    n_staged, n_gaps, n_unknown = con.execute(
        f"""SELECT count(*), count(*) FILTER (sched_dep IS NULL AND act_dep IS NULL
                                              AND stop_seq < max_seq),
                   count(*) FILTER (d.bpuic IS NULL)
            FROM (SELECT *, max(stop_seq) OVER (PARTITION BY service_day, trip_id) AS max_seq
                  FROM stg_stops WHERE service_day BETWEEN ? AND ?) s
            LEFT JOIN '{dim_station_parquet.as_posix()}' d ON d.bpuic = s.station_id""",
        [first, last],
    ).fetchone()
    return {
        "rides_train": stats[0],
        "rides_road": stats[1],
        "stops_additional_dropped": stats[2],
        "rides_malformed": stats[3],
        "rows_cancelled_side": stats[4],
        "ids_unparsed": unparsed,
        "stops_staged": n_staged,
        "stops_no_onward_leg": n_gaps,
        "stops_unknown_station": n_unknown,
    }


def gap_hours(first: date, last: date) -> list[str]:
    """UTC hours in [first, last] the crawler missed, as 'YYYY-MM-DDTHH'.

    Judged on scheduled stops per hour against the median of the same local hour of the
    week over the window, so a quiet night or a holiday is not a hole. Hours are counted
    in UTC, so the missing spring-forward hour is never mistaken for one.
    """
    import duckdb

    files = sorted(GERMANY.raw_dir.glob("data-*.parquet"))
    listed = "[" + ", ".join(f"'{f.as_posix()}'" for f in files) + "]"
    tz = GERMANY.timezone
    con = duckdb.connect()
    rows = con.execute(f"""
        WITH counted AS (
            SELECT date_trunc('hour', timezone('UTC', timezone('{tz}', CAST(
                       coalesce(departure_planned_time, arrival_planned_time) AS TIMESTAMP))))
                       AS utc_hour,
                   count(*) AS n
            FROM read_parquet({listed})
            WHERE NOT coalesce(is_additional_stop, false)
            GROUP BY 1
        ),
        -- Every hour of the window, so an hour with no row at all counts as zero.
        h AS (
            SELECT g.utc_hour, coalesce(c.n, 0) AS n,
                   timezone('{tz}', timezone('UTC', g.utc_hour)) AS local_hour
            FROM generate_series(TIMESTAMP '{first}', TIMESTAMP '{last} 23:00',
                                 INTERVAL 1 HOUR) g(utc_hour)
            LEFT JOIN counted c USING (utc_hour)
        ),
        m AS (
            SELECT *, median(n) OVER (PARTITION BY dayofweek(local_hour), hour(local_hour))
                          AS med
            FROM h
        )
        SELECT strftime(utc_hour, '%Y-%m-%dT%H') FROM m
        WHERE n < {GAP_HOUR_SHARE} * med
        ORDER BY 1""").fetchall()
    return [r[0] for r in rows]
