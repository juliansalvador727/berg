"""The German adapter on a hand-written month of the DB Timetables API archive.

Rows have the real file's shapes: zero-padded EVA strings, IRIS stop ids
`<hash>-<yymmddHHMM>-<position>` with negative hashes, naive Berlin local timestamps, change
times filled with the scheduled time when no realtime arrived, and per-side cancel flags.
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pytest

from berg_pipeline import ingest, paths
from berg_pipeline.europe import catalog, de, stops
from berg_pipeline.europe.config import GERMANY

DAY = date(2025, 12, 9)  # CET, UTC+1
STATIONS = [
    # (eva, name, ril100, lat, lon)
    (8002553, "Hamburg-Altona", "AA", 53.5520, 9.9350),
    (8002549, "Hamburg Hbf", "AH", 53.5530, 10.0067),
    (8000238, "Lüneburg", "ALBG", 53.2496, 10.4198),
    (8010310, "Salzwedel", "LSA", 52.8540, 11.1600),
    (8010334, "Stendal Hbf", "LS", 52.5946, 11.8551),
    (8011160, "Berlin Hbf", "BL", 52.5250, 13.3694),
    (8011113, "Berlin Südkreuz", "BPAF", 52.4757, 13.3657),
    (8000105, "Frankfurt (Main) Hbf", "FF", 50.1071, 8.6632),
    (8000349, "Darmstadt Hbf", "FD", 49.8725, 8.6293),
]
COLUMNS = (
    "station_name", "eva", "train_number", "line_number", "train_type", "id",
    "arrival_planned_time", "arrival_change_time", "departure_planned_time",
    "departure_change_time", "arrival_is_canceled", "departure_is_canceled",
    "is_additional_stop",
)


def stop(ride, pos, eva, tt, number, line=None, arr=None, dep=None, c_arr=None, c_dep=None,
         cancelled=False, additional=False, day=DAY):
    """arr/dep are 'HH:MM' planned local times on `day` (or 'YYYY-MM-DD HH:MM');
    change times default to planned, as the archive fills them."""
    def ts(v):
        if v is None:
            return None
        return v if len(v) > 5 else f"{day} {v}"
    return (f"s{eva}", f"{eva:08d}", number, line, tt, f"{ride}-{pos}",
            ts(arr), ts(c_arr or arr), ts(dep), ts(c_dep or dep),
            cancelled and arr is not None, cancelled and dep is not None, additional)


ICE = "-3851151041923621842-2512091836"
ROWS = [
    # ICE 599 Hamburg → Berlin with final predictions, a cancelled stop, and a stop IRIS
    # never lists between Stendal and Berlin.
    stop(ICE, 1, 8002553, "ICE", "599", dep="18:36", c_dep="18:44"),
    stop(ICE, 2, 8002549, "ICE", "599", arr="18:46", dep="18:49", c_arr="18:53", c_dep="18:56"),
    stop(ICE, 3, 8000238, "ICE", "599", arr="19:14", dep="19:16", c_arr="19:25", c_dep="19:28"),
    stop(ICE, 4, 8010310, "ICE", "599", arr="20:09", dep="20:10", cancelled=True),
    stop(ICE, 5, 8010334, "ICE", "599", arr="20:36", dep="20:37"),
    stop(ICE, 7, 8011160, "ICE", "599", arr="21:21"),
    # A stop added by a diversion: appended after the route, with a garbage scheduled time.
    stop(ICE, 8, 8011113, "ICE", "599", arr="21:30", dep="18:36", additional=True),
    # Replacement buses, under a bus type and under a rail operator's code.
    stop("111-2512091000", 1, 8000105, "Bus", "80001", line="SEV1", dep="10:00"),
    stop("111-2512091000", 2, 8000349, "Bus", "80001", line="SEV1", arr="10:40"),
    stop("112-2512091000", 1, 8000105, "VIA", "80002", line="SEV", dep="10:00"),
    stop("112-2512091000", 2, 8000349, "VIA", "80002", line="SEV", arr="10:40"),
    # One S-Bahn number in Hamburg and in Berlin on the same day.
    stop("4580333195418093259-2512091748", 1, 8011160, "S", "42183", line="S41", dep="17:48"),
    stop("4580333195418093259-2512091748", 2, 8011113, "S", "42183", line="S41", arr="17:58"),
    stop("298046680821961660-2512091804", 1, 8002553, "S", "42183", line="S1", dep="18:04"),
    stop("298046680821961660-2512091804", 2, 8002549, "S", "42183", line="S1", arr="18:12"),
    # Operator codes take their product from the line; without a line they keep the code.
    stop("-5-2512090700", 1, 8000105, "ag", "84001", line="RB33", dep="07:00"),
    stop("-5-2512090700", 2, 8000349, "ag", "84001", line="RB33", arr="07:20"),
    stop("-6-2512090800", 1, 8000105, "erx", "83001", dep="08:00"),
    stop("-6-2512090800", 2, 8000349, "erx", "83001", arr="08:25"),
]
# A ride of 31 December whose last stop is filed in January's file.
NYE = "77-2512312350"
DEC_ROWS = [stop(NYE, 1, 8000105, "RE", "4001", line="RE60", dep="23:50", day=date(2025, 12, 31))]
JAN_ROWS = [stop(NYE, 2, 8000349, "RE", "4001", line="RE60", arr="2026-01-01 00:20")]


def write_month(path: Path, rows: list) -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE t (station_name VARCHAR, eva VARCHAR, train_number VARCHAR, "
                "line_number VARCHAR, train_type VARCHAR, id VARCHAR, "
                "arrival_planned_time VARCHAR, arrival_change_time VARCHAR, "
                "departure_planned_time VARCHAR, departure_change_time VARCHAR, "
                "arrival_is_canceled BOOLEAN, departure_is_canceled BOOLEAN, "
                "is_additional_stop BOOLEAN)")
    con.executemany(f"INSERT INTO t VALUES ({', '.join('?' * len(COLUMNS))})", rows)
    times = ("arrival_planned_time", "arrival_change_time", "departure_planned_time",
             "departure_change_time")
    select = ", ".join(f"CAST({c} AS TIMESTAMP_NS) AS {c}" if c in times else c for c in COLUMNS)
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT {select} FROM t) TO '{path.as_posix()}' (FORMAT PARQUET)")


@pytest.fixture
def de_root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setattr(paths, "DATA_ROOT", tmp_path)
    write_month(de.raw_path("2025-12"), ROWS + DEC_ROWS)
    write_month(de.raw_path("2026-01"), JAN_ROWS)
    stations = [{"eva": e, "name": n, "code": c, "lon": lo, "lat": la}
                for e, n, c, la, lo in STATIONS]
    # An older snapshot with a stale name: the latest one wins.
    old = [dict(stations[0], name="Hamburg-Altona (alt)")]
    de.stations_path("2025-11").write_text(json.dumps({"stations": old}))
    de.stations_path("2025-12").write_text(json.dumps({"stations": stations}))
    return tmp_path


def built(first=DAY, last=DAY):
    dim = GERMANY.dim_station_parquet
    de.write_dim_station([de.stations_path("2025-11"), de.stations_path("2025-12")], dim)
    con = duckdb.connect()
    days = [first] if first == last else [first, last]
    staged = de.stage_days(con, days, dim)
    legs = stops.build_legs(con, GERMANY, first, last, dim)
    return con, staged, legs


def epoch(hh, mm, day=DAY) -> int:
    return int(datetime(day.year, day.month, day.day, hh, mm, tzinfo=timezone.utc)
               .timestamp()) - 3600


def legs_of(con, trip):
    return con.execute(
        "SELECT from_bpuic, to_bpuic, t_dep, dur, delay, flags FROM fct_legs "
        "WHERE trip_id = ? ORDER BY t_dep", [trip]
    ).fetchall()


def test_final_predictions_and_no_bridging(de_root):
    con, staged, legs = built()
    got = legs_of(con, "ICE 599")
    # Altona 18:44 → Hamburg Hbf 18:53, eight minutes late; then Hbf 18:56 → Lüneburg 19:25.
    assert got[0] == (8002553, 8002549, epoch(18, 44), 9 * 60, 8 * 60, 0)
    assert got[1] == (8002549, 8000238, epoch(18, 56), 29 * 60, 7 * 60, 0)
    # Salzwedel is cancelled and position 6 is missing: Lüneburg → Stendal and
    # Stendal → Berlin would each bridge a gap, so neither is drawn.
    assert len(got) == 2
    assert legs["not_run"] == 2
    assert staged["stops_additional_dropped"] == 1


def test_road_replacement_is_excluded(de_root):
    con, staged, _ = built()
    assert staged["rides_road"] == 2
    assert legs_of(con, "RB 80002") == []
    assert con.execute("SELECT count(*) FROM stg_stops WHERE train_number IN "
                       "('80001', '80002')").fetchone() == (0,)


def test_reused_numbers_get_their_own_journey(de_root):
    con, _, _ = built()
    assert [leg[:2] for leg in legs_of(con, "S 42183")] == [(8011160, 8011113)]
    assert [leg[:2] for leg in legs_of(con, "S 42183 #2")] == [(8002553, 8002549)]


def test_operator_codes_take_the_lines_product(de_root):
    con, _, _ = built()
    assert [leg[:2] for leg in legs_of(con, "RB 84001")] == [(8000105, 8000349)]
    assert [leg[:2] for leg in legs_of(con, "ERX 83001")] == [(8000105, 8000349)]
    assert con.execute("SELECT DISTINCT operator, line FROM stg_stops "
                       "WHERE trip_id = 'RB 84001'").fetchall() == [("ag", "RB33")]


def test_a_ride_crossing_into_the_next_months_file(de_root):
    nye = date(2025, 12, 31)
    con, _, _ = built(nye, nye)
    assert legs_of(con, "RE 4001") == [(8000105, 8000349, epoch(23, 50, day=nye), 1800, 0, 0)]


def test_latest_station_snapshot_wins(de_root):
    built()
    names = dict(duckdb.sql(
        f"SELECT bpuic, name FROM '{GERMANY.dim_station_parquet.as_posix()}'").fetchall())
    assert names[8002553] == "Hamburg-Altona"
    assert len(names) == len(STATIONS)


def test_manifest_carries_semantics_and_gap_hours(de_root):
    con, _, _ = built()
    legs_dir = GERMANY.publish_root / "legs"
    assert ingest.export_day(con, DAY, legs_dir / f"{DAY:%Y/%m/%d}.parquet")["rows"] > 0
    catalog.write_json({"source_gap_hours": ["2025-12-09T03"],
                        "source_revision": "rev"}, GERMANY.root / "quality.json")
    manifest = catalog.build_dataset_manifest(GERMANY)
    assert manifest["time_semantics"] == "final_prediction"
    assert manifest["license"] == "CC BY 4.0"
    assert manifest["source_gap_hours"] == ["2025-12-09T03"]
    assert "2025-11-03" in manifest["missing_days"]
