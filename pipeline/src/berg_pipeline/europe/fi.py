"""Finland adapter: Digitraffic's per-date train archive → normalized stop events.

One request per departure date returns every train with its full timetable, scheduled and
actual, as UTC ISO timestamps (see docs/data-notes-fi.md). The raw response is kept gzipped
under data/datasets/fi/raw only until the dataset is validated and published; it never goes
to R2.

Normalization rules:

  - Passenger services only: trainCategory Commuter or Long-distance. Cargo, shunting,
    locomotive, test and on-track-machine movements are dropped with the train.
  - Commercial stops only (trainStopping AND commercialStop). Timing points a train passes
    are operational, not stations a passenger can use, and Switzerland publishes stops only.
  - A time is an observation only when actualTime is present. liveEstimateTime is a
    prediction and is never read.
  - Cancelled trains and cancelled stop rows are dropped, counted in the stage stats.
  - Stop order is the source's row order. Unlike Ist-Daten, the API gives a real sequence, so
    a delayed train cannot reorder its own stops.
"""

import gzip
import json
import os
import time
from datetime import date
from pathlib import Path

from berg_pipeline import ingest
from berg_pipeline.europe.config import FINLAND

API = "https://rata.digitraffic.fi/api/v1"
# Digitraffic asks every client to identify itself; anonymous clients are throttled harder.
USER_AGENT_HEADERS = {"Digitraffic-User": "berg/europe-archive", "Accept-Encoding": "gzip"}
PASSENGER_CATEGORIES = ("Commuter", "Long-distance")


def raw_path(day: date) -> Path:
    return FINLAND.raw_dir / f"{day:%Y}" / f"{day:%Y-%m-%d}.json.gz"


def _get(client, url: str, attempts: int = 6):
    for attempt in range(1, attempts + 1):
        try:
            response = client.get(url)
            if response.status_code == 429 or response.status_code >= 500:
                raise RuntimeError(f"HTTP {response.status_code}")
            response.raise_for_status()
            return response
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(min(60, 2**attempt))
    raise AssertionError("unreachable")


def fetch_day(day: date, client=None, force: bool = False) -> Path:
    """Download one departure date's trains, atomically. An existing file is trusted.

    The body is validated as a JSON array of trains for exactly this departureDate before it
    is renamed into place, so an interrupted or truncated download can never be mistaken for a
    quiet day on the next run.
    """
    import httpx

    out = raw_path(day)
    if out.exists() and not force:
        return out
    own = client is None
    client = client or httpx.Client(headers=USER_AGENT_HEADERS, timeout=120)
    try:
        response = _get(client, f"{API}/trains/{day.isoformat()}")
        trains = response.json()
    finally:
        if own:
            client.close()
    if not isinstance(trains, list):
        raise RuntimeError(f"{day}: expected a JSON array, got {type(trains).__name__}")
    wrong = [t.get("departureDate") for t in trains if t.get("departureDate") != day.isoformat()]
    if wrong:
        raise RuntimeError(f"{day}: {len(wrong)} trains carry another departureDate: {wrong[:3]}")

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".{out.name}.tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(trains, fh, separators=(",", ":"))
    os.replace(tmp, out)
    return out


def fetch_stations(client=None) -> list[dict]:
    import httpx

    own = client is None
    client = client or httpx.Client(headers=USER_AGENT_HEADERS, timeout=120)
    try:
        return _get(client, f"{API}/metadata/stations").json()
    finally:
        if own:
            client.close()


def fetch_train_types(client=None) -> list[dict]:
    import httpx

    own = client is None
    client = client or httpx.Client(headers=USER_AGENT_HEADERS, timeout=120)
    try:
        return _get(client, f"{API}/metadata/train-types").json()
    finally:
        if own:
            client.close()


# Explicit column types: read_json's autodetect types per file, and a column that is null for
# a whole day would otherwise come back as a different type than it did yesterday. Timestamps
# stay text until they are parsed as UTC below.
_TRAIN_COLUMNS = """{
    departureDate: 'DATE',
    trainNumber: 'BIGINT',
    trainType: 'VARCHAR',
    trainCategory: 'VARCHAR',
    commuterLineID: 'VARCHAR',
    operatorShortCode: 'VARCHAR',
    cancelled: 'BOOLEAN',
    version: 'BIGINT',
    timeTableRows: 'STRUCT(
        "type" VARCHAR, cancelled BOOLEAN, scheduledTime VARCHAR, actualTime VARCHAR,
        commercialStop BOOLEAN, trainStopping BOOLEAN, stationUICCode BIGINT,
        countryCode VARCHAR)[]'
}"""


def local_station_id(country: str, uic: int) -> int:
    """The dataset-local station id. A Digitraffic UIC code is unique only WITHIN a country:
    1000 is both Ahvenus (FI) and Petrozavodsk (RU) in the live metadata. Finnish stations keep
    their bare code; foreign ones are lifted into a country-specific range above it.
    """
    if country == "FI":
        return int(uic)
    if len(country) != 2 or not country.isalpha():
        raise ValueError(f"unexpected countryCode {country!r}")
    a, b = (ord(c) - 64 for c in country.upper())
    return (a * 100 + b) * 1_000_000 + int(uic)


def _local_station_sql(country: str, uic: str) -> str:
    """local_station_id in SQL; the two must agree (tested)."""
    return (
        f"CASE WHEN {country} = 'FI' THEN {uic} ELSE "
        f"((ascii(upper({country})[1]) - 64) * 100 + (ascii(upper({country})[2]) - 64)) "
        f"* 1000000 + {uic} END"
    )


def _utc(expr: str) -> str:
    """ISO-8601 'YYYY-MM-DDTHH:MM:SS.sssZ' → epoch seconds. The Z is honoured by TIMESTAMPTZ."""
    return f"CAST(epoch(CAST({expr} AS TIMESTAMPTZ)) AS BIGINT)"


def stage_days(con, days: list[date]) -> dict:
    """Raw per-date JSON → stg_stops for exactly these service days. Idempotent per day."""
    from berg_pipeline.europe import stops

    stops.create_tables(con)
    files = [raw_path(d) for d in days]
    missing = [f.name for f in files if not f.exists()]
    if missing:
        raise RuntimeError(f"raw Digitraffic days not fetched: {missing[:5]}")
    listed = "[" + ", ".join(f"'{f.as_posix()}'" for f in files) + "]"
    cats = ", ".join(f"'{c}'" for c in PASSENGER_CATEGORIES)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _fi_trains AS
        SELECT * FROM read_json({listed}, format='array', columns={_TRAIN_COLUMNS},
                                maximum_object_size=16777216)""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _fi_rows AS
        WITH u AS (
            SELECT departureDate, trainNumber, trainType, commuterLineID, operatorShortCode,
                   version,
                   generate_subscripts(timeTableRows, 1) - 1 AS i,
                   unnest(timeTableRows) AS r
            FROM _fi_trains
            WHERE trainCategory IN ({cats}) AND NOT coalesce(cancelled, false)
        )
        SELECT * EXCLUDE (r), unnest(r) FROM u""")

    # The API's shape: row 0 is the origin's DEPARTURE, then ARRIVAL/DEPARTURE pairs, then the
    # terminus's ARRIVAL — so stop k owns rows 2k-1 and 2k. Verified on every staged day; a
    # train that breaks it is excluded whole and counted rather than guessed at.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _fi_malformed AS
        SELECT DISTINCT departureDate, trainNumber FROM _fi_rows
        WHERE ("type" <> CASE WHEN i % 2 = 0 THEN 'DEPARTURE' ELSE 'ARRIVAL' END)
        UNION
        SELECT departureDate, trainNumber FROM _fi_rows
        GROUP BY departureDate, trainNumber, (i + 1) // 2
        HAVING count(DISTINCT (countryCode, stationUICCode)) > 1""")

    first, last = min(days), max(days)
    with ingest._transaction(con):
        # What the source itself says about each day. A day where nearly every train is
        # cancelled (a national strike) is thin because the railway stopped, not because the
        # ingest collapsed — and this is the evidence that tells the two apart.
        con.execute(
            "DELETE FROM source_days WHERE service_day BETWEEN ? AND ?", [first, last]
        )
        con.execute(f"""
            INSERT INTO source_days
            SELECT departureDate, count(*), count(*) FILTER (coalesce(cancelled, false))
            FROM _fi_trains
            WHERE trainCategory IN ({cats}) AND departureDate BETWEEN DATE '{first}' AND DATE '{last}'
            GROUP BY departureDate""")
        con.execute(
            "DELETE FROM stg_stops WHERE service_day BETWEEN ? AND ?", [first, last]
        )
        con.execute(f"""
            INSERT INTO stg_stops
            WITH live AS (
                SELECT r.* FROM _fi_rows r
                ANTI JOIN _fi_malformed m USING (departureDate, trainNumber)
                WHERE NOT coalesce(r.cancelled, false)
            ),
            per_stop AS (
                SELECT departureDate, trainNumber,
                       (i + 1) // 2                                        AS stop_seq,
                       any_value(trainType)                                AS train_type,
                       any_value(commuterLineID)                           AS commuter_line,
                       any_value(operatorShortCode)                        AS operator,
                       any_value(version)                                  AS version,
                       any_value({_local_station_sql('countryCode', 'stationUICCode')})
                                                                           AS station_id,
                       bool_or(coalesce(trainStopping, false)
                               AND coalesce(commercialStop, false))        AS commercial,
                       max(scheduledTime) FILTER ("type" = 'ARRIVAL')      AS s_arr,
                       max(actualTime)    FILTER ("type" = 'ARRIVAL')      AS a_arr,
                       max(scheduledTime) FILTER ("type" = 'DEPARTURE')    AS s_dep,
                       max(actualTime)    FILTER ("type" = 'DEPARTURE')    AS a_dep
                FROM live
                GROUP BY departureDate, trainNumber, (i + 1) // 2
            )
            SELECT departureDate                                AS service_day,
                   train_type || ' ' || trainNumber             AS trip_id,
                   operator,
                   CAST(trainNumber AS VARCHAR)                 AS train_number,
                   train_type                                   AS category,
                   coalesce(nullif(trim(commuter_line), ''),
                            train_type || ' ' || trainNumber)   AS line,
                   station_id,
                   stop_seq,
                   {_utc('s_arr')}                              AS sched_arr,
                   {_utc('s_dep')}                              AS sched_dep,
                   {_utc('a_arr')}                              AS act_arr,
                   {_utc('a_dep')}                              AS act_dep,
                   a_arr IS NOT NULL                            AS arr_measured,
                   a_dep IS NOT NULL                            AS dep_measured,
                   version                                      AS source_revision
            FROM per_stop
            WHERE commercial AND departureDate BETWEEN DATE '{first}' AND DATE '{last}'""")

    stats = con.execute("""
        SELECT (SELECT count(*) FROM _fi_trains),
               (SELECT count(*) FROM _fi_trains
                WHERE trainCategory IN ('Commuter', 'Long-distance')),
               (SELECT count(*) FROM _fi_trains
                WHERE trainCategory IN ('Commuter', 'Long-distance')
                  AND coalesce(cancelled, false)),
               (SELECT count(*) FROM _fi_malformed),
               (SELECT count(*) FROM _fi_rows WHERE coalesce(cancelled, false))""").fetchone()
    n_staged = con.execute(
        "SELECT count(*) FROM stg_stops WHERE service_day BETWEEN ? AND ?", [first, last]
    ).fetchone()[0]
    return {
        "trains_raw": stats[0],
        "trains_passenger": stats[1],
        "trains_cancelled": stats[2],
        "trains_malformed": stats[3],
        "rows_cancelled": stats[4],
        "stops_staged": n_staged,
    }


def write_dim_station(stations: list[dict], out: Path) -> dict:
    """Digitraffic station metadata → the dim_station parquet shape the leg builder and the
    geometry job both read (bpuic = the dataset-local id, here the Finnish UIC code).

    The metadata endpoint is a current snapshot with no history, so every row is valid for all
    time. Finnish stations are keyed by a stable numeric UIC code rather than renumbered, and
    the coordinates of a station that moved did so by metres.
    """
    import duckdb

    rows = [
        (local_station_id(s["countryCode"], s["stationUICCode"]), s["stationName"], s["stationShortCode"],
         float(s["longitude"]), float(s["latitude"]), bool(s["passengerTraffic"]),
         s["countryCode"])
        for s in stations
    ]
    ids = [r[0] for r in rows]
    if len(ids) != len(set(ids)):
        raise RuntimeError("duplicate (countryCode, stationUICCode) in station metadata")
    out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("""CREATE TABLE s (bpuic BIGINT, name VARCHAR, code VARCHAR, lon DOUBLE,
                                   lat DOUBLE, passenger BOOLEAN, country VARCHAR)""")
    con.executemany("INSERT INTO s VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"""
        COPY (SELECT *, DATE '1900-01-01' AS valid_from, DATE '9999-12-31' AS valid_to
              FROM s ORDER BY bpuic)
        TO '{out.as_posix()}' (FORMAT PARQUET)""")
    return {"stations": len(rows)}
