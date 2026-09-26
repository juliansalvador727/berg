"""The Austrian adapter on a hand-written timetable and train-run week.

Each fixture train encodes one rule from at.py's docstring. Runs have the real file's columns
and shapes: planned and actual times to the second with a +0100 offset, at operating points
that are not the timetable's termini.
"""

from datetime import date, datetime, timezone

import duckdb
import pytest

from berg_pipeline import paths
from berg_pipeline.europe import at, catalog, stops
from berg_pipeline.europe.config import AUSTRIA

DAY = date(2026, 2, 10)  # a Tuesday, CET (UTC+1)
STOPS = [
    # (stop place, name, lat, lon)
    ("at:49:1", "Wien Hbf", 48.185, 16.377),
    ("at:43:2", "St. Pölten Hbf", 48.208, 15.624),
    ("at:43:3", "Amstetten", 48.123, 14.873),
    ("at:44:4", "Linz Hbf", 48.290, 14.291),
    ("at:44:5", "Wels Hbf", 48.166, 14.027),
    ("de:09162:6", "München Hbf", 48.140, 11.558),
]
TRIPS = [
    # (trip_id, short name, [(stop place, arr, dep, passing)])
    # RJX 20: observed +1 min at the start point, +3 min at the end point.
    ("t20", "RJX 20", [("at:49:1", "10:00:00", "10:00:00", False),
                       ("at:43:2", "10:30:00", "10:32:00", False),
                       ("at:43:3", "10:50:00", "10:50:00", True),
                       ("at:44:4", "11:14:00", "11:14:00", False)]),
    # RJX 21: its start observation is a registration artefact (3 h early), so the end one
    # applies throughout.
    ("t21", "RJX 21", [("at:44:4", "12:00:00", "12:00:00", False),
                       ("at:43:2", "12:40:00", "12:42:00", False),
                       ("at:49:1", "13:14:00", "13:14:00", False)]),
    # R 5000 is timetabled but ÖBB recorded no run: not published.
    ("t5000", "R 5000", [("at:44:4", "14:00:00", "14:00:00", False),
                         ("at:44:5", "14:20:00", "14:20:00", False)]),
    # REX 7: the number runs twice that day; each run pairs with the trip it overlaps.
    ("t7a", "REX 7", [("at:44:4", "06:00:00", "06:00:00", False),
                      ("at:44:5", "06:20:00", "06:20:00", False)]),
    ("t7b", "REX 7", [("at:44:4", "18:00:00", "18:00:00", False),
                      ("at:44:5", "18:20:00", "18:20:00", False)]),
    # EC 111 continues to München: the foreign leg is clipped, not quarantined.
    ("t111", "EC 111", [("at:44:4", "15:00:00", "15:00:00", False),
                        ("at:44:5", "15:15:00", "15:17:00", False),
                        ("de:09162:6", "17:30:00", "17:30:00", False)]),
]
RUNS = [
    # (number, planned start, actual start, planned end, actual end)
    ("20", "10:01:00", "10:02:00", "11:12:00", "11:15:00"),
    ("21", "12:01:00", "09:01:00", "13:12:00", "13:14:00"),
    ("7", "18:00:30", "18:05:30", "18:19:00", "18:24:00"),
    ("7", "06:00:30", "06:00:30", "06:19:00", "06:19:00"),
    ("111", "15:00:30", "15:00:30", "15:30:00", "15:30:00"),
    # 45123 is empty stock: a run with no trip draws nothing.
    ("45123", "08:00:00", "08:00:00", "08:30:00", "08:30:00"),
]


def ts(hms: str) -> str:
    return f"{DAY.isoformat()}T{hms}+0100"


def epoch(hms: str) -> int:
    h, m, s = map(int, hms.split(":"))
    return int(datetime(DAY.year, DAY.month, DAY.day, h - 1, m, s, tzinfo=timezone.utc).timestamp())


def write(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [header] + [",".join(f'"{v}"' for v in r) for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture
def at_root(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "DATA_ROOT", tmp_path)
    g = at.gtfs_dir(2026)
    write(g / "agency.txt", "agency_id,agency_name,agency_url,agency_timezone",
          [("01", "OEBB Personenverkehr AG", "https://www.oebb.at", "Europe/Vienna")])
    write(g / "routes.txt", "route_id,agency_id,route_short_name,route_long_name,route_type",
          [("r1", "01", "X", "Line", "2")])
    write(g / "calendar.txt",
          "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date",
          [("TA", 1, 1, 1, 1, 1, 1, 1, "20251214", "20261212")])
    write(g / "calendar_dates.txt", "service_id,date,exception_type", [("TA", "20260101", "2")])
    write(g / "trips.txt", "route_id,service_id,trip_id,trip_short_name",
          [("r1", "TA", tid, name) for tid, name, _ in TRIPS])
    write(g / "stop_times.txt",
          "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type",
          [(tid, a, d, f"{sp}:0:1", i, int(p), int(p))
           for tid, _, sts in TRIPS for i, (sp, a, d, p) in enumerate(sts, 1)])
    write(g / "stops.txt", "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station",
          [row for sp, name, lat, lon in STOPS
           for row in ((f"P{sp}", name, lat, lon, "1", ""), (f"{sp}:0:1", name, lat, lon, "", f"P{sp}"))])
    runs = at.raw_runs_dir() / "2026" / "2026-02-23_mmtis_zugfahrten.csv"
    runs.parent.mkdir(parents=True, exist_ok=True)
    runs.write_text(at.RUNS_HEADER + "\n" + "\n".join(
        f"{n},{DAY},{ts(pa)},{ts(aa)},XA,{ts(pb)},{ts(ab)},XB," for n, pa, aa, pb, ab in RUNS
    ) + "\n", encoding="utf-8")

    at.write_dim_station([g], AUSTRIA.dim_station_parquet)
    con = duckdb.connect()
    stats = at.stage_days(con, [DAY], AUSTRIA.dim_station_parquet, [g])
    legs = stops.build_legs(con, AUSTRIA, DAY, DAY, AUSTRIA.dim_station_parquet)
    return con, stats, legs


def stop_times(con, trip):
    return con.execute(
        "SELECT station_id, sched_arr, act_arr, sched_dep, act_dep FROM stg_stops "
        "WHERE trip_id = ? ORDER BY stop_seq", [trip]).fetchall()


def test_delay_is_interpolated_between_the_two_observations(at_root):
    con, _, _ = at_root
    rows = stop_times(con, "RJX 20")
    assert [r[0] for r in rows] == [at.station_id(s) for s in ("at:49:1", "at:43:2", "at:44:4")]
    # Before the start point's planned 10:01 the start delay (+60 s) holds.
    assert rows[0][4] - rows[0][3] == 60
    # 10:30 lies 29/71 of the way from 10:01 (+60 s) to 11:12 (+180 s).
    assert rows[1][2] - rows[1][1] == round(60 + 120 * 29 / 71)
    # After the end point's planned 11:12 the end delay (+180 s) holds.
    assert rows[2][2] - rows[2][1] == 180
    assert rows[0][3] == epoch("10:00:00")


def test_passing_rows_are_not_stops(at_root):
    con, _, _ = at_root
    assert at.station_id("at:43:3") not in [r[0] for r in stop_times(con, "RJX 20")]


def test_an_implausible_observation_yields_to_the_other(at_root):
    con, _, _ = at_root
    rows = stop_times(con, "RJX 21")
    assert all(r[4] - r[3] == 120 for r in rows if r[3] is not None)


def test_only_trips_recorded_running_are_published(at_root):
    con, stats, _ = at_root
    trips = {r[0] for r in con.execute("SELECT DISTINCT trip_id FROM stg_stops").fetchall()}
    assert trips == {"RJX 20", "RJX 21", "REX 7", "REX 7 #2", "EC 111"}
    assert stats["runs_without_trip"] == 1 and stats["trips_without_run"] == 1


def test_a_reused_number_pairs_by_overlap(at_root):
    con, _, _ = at_root
    morning = stop_times(con, "REX 7")
    evening = stop_times(con, "REX 7 #2")
    assert morning[0][4] - morning[0][3] == 0
    assert evening[0][4] - evening[0][3] == 300


def test_foreign_legs_are_clipped_not_quarantined(at_root):
    _, _, legs = at_root
    assert legs["outside_country"] == 1
    assert set(legs) <= {"ok", "legs_written", "outside_country"}


def test_manifest_labels_the_evidence(at_root):
    manifest = catalog.build_dataset_manifest(AUSTRIA)
    assert manifest["time_semantics"] == "delay_interpolated"
    assert manifest["license"].startswith("CC BY 3.0 AT")
