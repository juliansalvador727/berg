"""The Belgian adapter on a hand-written Infrabel month.

Rows have the real file's columns and shapes: DDMONYYYY dates per side, H:MM:SS local times
without a leading zero, THOP1_COD pass codes, and empty fields where a side does not exist.
The ptcar list is semicolon-separated with a byte-order mark, like the real export.
"""

import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from berg_pipeline import ingest, paths
from berg_pipeline.europe import be, build, catalog, stops
from berg_pipeline.europe.config import BELGIUM

DAY = date(2024, 3, 12)  # CET, UTC+1
PTCARS = [
    # (ptcarid, french, dutch, class_en, lat, lon)
    (100, "Bruges", "Brugge", "Station", 51.1972, 3.2169),
    (101, "Oostkamp", "Oostkamp", "Stop in open track", 51.1554, 3.2394),
    (102, "Gand-Saint-Pierre", "Gent-Sint-Pieters", "Station", 51.0357, 3.7106),
    (103, "Y.Gent-Oost", "Y.Gent-Oost", "Junction", 51.0400, 3.7500),
    (104, "Bruxelles-Midi", "Brussel-Zuid", "Station", 50.8358, 4.3363),
    (105, "Liège-Guillemins", "Luik-Guillemins", "Station", 50.6245, 5.5667),
]


def d(day: date) -> str:
    return day.strftime("%d%b%Y").upper()


def row(number, relation, ptcar, code, arr=None, dep=None, act_arr=None, act_dep=None,
        arr_day=DAY, dep_day=DAY, service_day=DAY):
    """arr/dep are 'H:MM:SS' planned times; act_* default to planned."""
    act_arr = act_arr or arr
    act_dep = act_dep or dep
    return [
        d(service_day), number, relation, "SNCB/NMBS", ptcar, code or "", "50A",
        act_arr or "", act_dep or "", arr or "", dep or "", "", "", "1", relation, "X", "50A",
        d(arr_day) if arr else "", d(dep_day) if dep else "",
        d(arr_day) if arr else "", d(dep_day) if dep else "",
    ]


ROWS = [
    # IC 1509: origin at Bruges, runs through Oostkamp, stops at Ghent and passes a junction
    # the source lists with a stop code, terminates at Brussels.
    row("1509", "IC 03", 100, None, dep="9:53:00", act_dep="9:53:30"),
    row("1509", "IC 03", 101, "D", arr="10:01:00", dep="10:01:00"),
    row("1509", "IC 03", 102, "=", arr="10:20:00", dep="10:23:00",
        act_arr="10:21:10", act_dep="10:24:00"),
    row("1509", "IC 03", 103, "=", arr="10:25:00", dep="10:25:00"),
    row("1509", "IC 03", 104, None, arr="10:55:00", act_arr="10:58:00"),
    # L 5571 departs 23:50 and arrives after midnight: the arrival carries tomorrow's date.
    row("5571", "L 15", 102, None, dep="23:50:00"),
    row("5571", "L 15", 104, None, arr="0:20:00", arr_day=DAY + timedelta(days=1)),
    # An EXTRA train has no THOP1 code at any stop, and every stop is still a stop.
    row("17213", "EXTRA", 100, None, dep="11:00:00"),
    row("17213", "EXTRA", 102, None, arr="11:30:00", dep="11:31:00"),
    row("17213", "EXTRA", 104, None, arr="12:00:00"),
    # One number, two runs on one day (Brussels twice): excluded whole.
    row("4000", "L 30", 104, None, dep="13:00:00"),
    row("4000", "L 30", 102, "=", arr="13:30:00", dep="13:32:00"),
    row("4000", "L 30", 104, None, arr="14:00:00"),
    # A lettered local relation is a suburban S line.
    row("3700", "L B1-1", 102, None, dep="16:00:00"),
    row("3700", "L B1-1", 104, None, arr="16:40:00"),
    row("3701", "S1-2", 102, None, dep="16:30:00"),
    row("3701", "S1-2", 104, None, arr="17:10:00"),
    # Two stops planned for the same minute: the actual times give the order (102 before
    # 101, against the ptcar ids).
    row("6600", "L 50", 100, None, dep="7:00:00"),
    row("6600", "L 50", 101, "=", arr="7:05:00", dep="7:05:00",
        act_arr="7:05:50", act_dep="7:05:50"),
    row("6600", "L 50", 102, "=", arr="7:05:00", dep="7:05:00",
        act_arr="7:05:10", act_dep="7:05:10"),
    row("6600", "L 50", 104, None, arr="7:30:00"),
    # A train the source gives no relation.
    row("22221", "", 100, None, dep="17:00:00"),
    row("22221", "", 102, None, arr="17:30:00"),
    # A ptcar missing from the snapshot is quarantined as unmatched, not silently dropped.
    row("9000", "ICE", 105, None, dep="15:00:00"),
    row("9000", "ICE", 999, None, arr="15:30:00"),
]


@pytest.fixture
def be_root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setattr(paths, "DATA_ROOT", tmp_path)
    raw = be.raw_path(f"{DAY:%Y-%m}")
    raw.parent.mkdir(parents=True)
    with open(raw, "w", newline="") as fh:
        fh.write(be.HEADER + "\n")
        csv.writer(fh, lineterminator="\n").writerows(ROWS)
    with open(be.stations_path(), "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh, delimiter=";", lineterminator="\n")
        writer.writerow([
            "geo_point_2d", "geo_shape", "ptcarid", "taftapcode", "symbolicname",
            "shortnamefrench", "shortnamedutch", "longnamefrench", "longnamedutch",
            "commercialshortnamefrench", "commercialshortnamedutch",
            "commercialmiddlenamefrench", "commercialmiddlenamedutch",
            "commerciallongnamefrench", "commerciallongnamedutch",
            "classification", "class_en", "class_fr",
        ])
        for pid, fr, nl, cls, lat, lon in PTCARS:
            writer.writerow([f"{lat}, {lon}", "{}", pid, f"BE{pid:05d}", f"F{pid}",
                             fr, nl, fr, nl, fr, nl, fr, nl, fr, nl, cls, cls, cls])
    return tmp_path


def built(be_root):
    dim = BELGIUM.dim_station_parquet
    be.write_dim_station(be.stations_path(), dim)
    con = duckdb.connect()
    staged = be.stage_days(con, [DAY], dim)
    legs = stops.build_legs(con, BELGIUM, DAY, DAY, dim)
    return con, staged, legs


def epoch(hh, mm, ss=0, day=DAY) -> int:
    return int(datetime(day.year, day.month, day.day, hh, mm, ss, tzinfo=timezone.utc)
               .timestamp()) - 3600


def legs_of(con, trip):
    return con.execute(
        "SELECT from_bpuic, to_bpuic, t_dep, dur, delay, flags FROM fct_legs "
        "WHERE trip_id = ? ORDER BY t_dep", [trip]
    ).fetchall()


def test_observed_times_skip_passes_and_operational_points(be_root):
    con, _, _ = built(be_root)
    legs = legs_of(con, "IC 1509")
    # Bruges dep 9:53:30 actual → Ghent arr 10:21:10 actual; Oostkamp (D) is a pass.
    assert legs[0] == (100, 102, epoch(9, 53, 30), 27 * 60 + 40, 30, 0)
    # The junction coded "=" is not a stop, so Ghent → Brussels is one leg.
    assert legs[1][:4] == (102, 104, epoch(10, 24), 34 * 60)
    assert legs[1][4] == 60


def test_cross_midnight_uses_each_sides_own_date(be_root):
    con, _, _ = built(be_root)
    assert legs_of(con, "L 5571") == [(102, 104, epoch(23, 50), 30 * 60, 0, 0)]


def test_extra_trains_without_codes_keep_every_stop(be_root):
    con, _, _ = built(be_root)
    assert [leg[:2] for leg in legs_of(con, "EXTRA 17213")] == [(100, 102), (102, 104)]


def test_lettered_local_relations_are_suburban(be_root):
    con, _, _ = built(be_root)
    assert [leg[:2] for leg in legs_of(con, "S 3700")] == [(102, 104)]
    assert [leg[:2] for leg in legs_of(con, "S 3701")] == [(102, 104)]
    assert con.execute(
        "SELECT line FROM stg_stops WHERE trip_id = 'S 3700' LIMIT 1").fetchone() == ("L B1-1",)


def test_stops_planned_for_the_same_minute_follow_actual_order(be_root):
    con, _, _ = built(be_root)
    assert [leg[:2] for leg in legs_of(con, "L 6600")] == [(100, 102), (102, 101), (101, 104)]


def test_a_train_without_relation_is_generic(be_root):
    con, _, _ = built(be_root)
    assert [leg[:2] for leg in legs_of(con, "TRAIN 22221")] == [(100, 102)]


def test_one_number_running_twice_is_excluded_whole(be_root):
    con, staged, _ = built(be_root)
    assert staged["trains_malformed"] == 1
    assert legs_of(con, "L 4000") == []


def test_unknown_ptcar_is_quarantined(be_root):
    con, staged, legs = built(be_root)
    assert staged["stops_unknown_station"] == 1
    assert legs["unmatched_station"] == 1
    assert con.execute("SELECT reason FROM quarantine_legs").fetchall() == [("unmatched_station",)]


def test_months_with_reordered_columns_are_read_by_name(be_root):
    """2025-08 reorders the columns and 2025-09 adds one; a positional read of a month list
    would silently shift every field."""
    nxt = date(2024, 4, 1)
    layout = ["DATDEP", "CIRC_TYP", "TRAIN_NO", "RELATION", "TRAIN_SERV", "OP1_COD"] + [
        c for c in be.HEADER.split(",")
        if c not in ("DATDEP", "CIRC_TYP", "TRAIN_NO", "RELATION", "TRAIN_SERV")]
    old = be.HEADER.split(",")
    rows = [row("1601", "IC 01", 100, None, dep="8:00:00", service_day=nxt, dep_day=nxt),
            row("1601", "IC 01", 102, None, arr="8:30:00", service_day=nxt, arr_day=nxt)]
    with open(be.raw_path("2024-04"), "w", newline="") as fh:
        fh.write(",".join(layout) + "\n")
        writer = csv.writer(fh, lineterminator="\n")
        for r in rows:
            by_name = dict(zip(old, r)) | {"OP1_COD": ""}
            writer.writerow([by_name[c] for c in layout])
    dim = BELGIUM.dim_station_parquet
    be.write_dim_station(be.stations_path(), dim)
    con = duckdb.connect()
    be.stage_days(con, [date(2024, 3, 31), nxt], dim)
    stops.build_legs(con, BELGIUM, date(2024, 3, 31), nxt, dim)
    assert legs_of(con, "IC 1601") == [(100, 102, epoch(8, 0, day=nxt) - 3600, 1800, 0, 0)]


def test_stations_are_bilingual_and_passenger_only(be_root):
    built(be_root)
    build.write_stations_json(BELGIUM, passenger_only=True)
    published = json.loads((BELGIUM.publish_root / "static" / "stations.json").read_text())
    names = {s["id"]: s["name"] for s in published}
    assert 103 not in names  # the junction
    assert names[101] == "Oostkamp"
    assert names[104] == "Bruxelles-Midi / Brussel-Zuid"


def test_exports_and_manifest(be_root):
    con, _, _ = built(be_root)
    legs_dir = BELGIUM.publish_root / "legs"
    assert ingest.export_day(con, DAY, legs_dir / f"{DAY:%Y/%m/%d}.parquet")["rows"] > 0
    manifest = catalog.build_dataset_manifest(BELGIUM)
    assert manifest["time_semantics"] == "observed"
    assert manifest["license"] == "CC0 1.0"
    assert manifest["punctuality_threshold_s"] == 360
    assert "2024-03-11" in manifest["missing_days"]
