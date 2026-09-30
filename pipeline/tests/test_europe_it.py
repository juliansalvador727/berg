"""The Italian adapter, on hand-written TrainStats day files.

Each fixture train encodes one rule from it.py's docstring. Files are the archive's shape: a
ZIP holding one JSON day ({giorno, timeZone, riassunto, treni}), stop times as UTC epoch
seconds, delays as strings of whole minutes or the markers N, S and n.d.
"""

import io
import json
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pytest

from berg_pipeline import paths
from berg_pipeline.europe import it, it_fetch, stops
from berg_pipeline.europe.config import ITALY

# name → (code, lat, lon): a straight north-south line, stations ~5.5 km apart.
LINE = {
    "ALFA": ("S00001", 45.00, 9.0),
    "BETA": ("S00002", 45.05, 9.0),
    "GAMMA": ("S00003", 45.10, 9.0),
    "DELTA": ("S00004", 45.15, 9.0),
    "EPSILON": ("S00005", 45.20, 9.0),
}


def utc(y, mo, d, h, mi) -> int:
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp())


def stop(name, oa, op, ra, rp):
    return {"n": name, "ra": ra, "rp": rp, "oa": oa, "op": op}


def train(number, stops_, cat="REG", sub=None, rp=None, ra=None, **extra):
    first = next((s for s in stops_ if s["op"]), None)
    last = next((s for s in reversed(stops_) if s["oa"]), None)
    t = {
        "_id": f"{number}-{first['op'] if first else 0}-QUxGQQ==",
        "n": str(number),
        "p": stops_[0]["n"] if stops_ else "ALFA",
        "rp": rp if rp is not None else (stops_[0]["rp"] if stops_ else "X"),
        "a": stops_[-1]["n"] if stops_ else "EPSILON",
        "ra": ra if ra is not None else (stops_[-1]["ra"] if stops_ else "X"),
        "c": cat,
        "op": first["op"] if first else 0,
        "oa": last["oa"] if last else 0,
        "fr": stops_,
    }
    if sub:
        t["sub"] = sub
    t.update(extra)
    return t


def write_day(day: date, trains: list[dict]) -> None:
    doc = {
        "giorno": f"{day:%d/%m/%Y}",
        "timeZone": 7200,
        "riassunto": {"treniCircolati": len(trains), "treniMonitorati": len(trains)},
        "treni": trains,
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(f"dati_{day:%Y_%m_%d}.json", json.dumps(doc))
    out = it_fetch.raw_root(ITALY) / "days" / f"dati_{day:%Y_%m_%d}.zip"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(buf.getvalue())


def write_station_lists(extra_osm: dict[str, tuple[float, float]] | None = None) -> None:
    d = it.station_dir()
    d.mkdir(parents=True, exist_ok=True)
    registry = [
        {
            "codStazione": code,
            "lat": lat,
            "lon": lon,
            "esterno": False,
            "localita": {"nomeLungo": name, "nomeBreve": name.title()},
        }
        for name, (code, lat, lon) in LINE.items()
    ]
    (d / "elencoStazioni_0.json").write_text(json.dumps(registry))
    (d / "autocompleta_A.txt").write_text(
        "\n".join(f"{n}|{c}" for n, (c, _, _) in LINE.items()) + "\n"
    )
    osm = [
        {"type": "node", "lat": lat, "lon": lon, "tags": {"name": name.title()}}
        for name, (_, lat, lon) in LINE.items()
    ]
    for name, (lat, lon) in (extra_osm or {}).items():
        osm.append({"type": "node", "lat": lat, "lon": lon, "tags": {"name": name}})
    (d / it.OSM_STATIONS).write_text(json.dumps({"elements": osm}))
    (ITALY.raw_dir / it.FIGSHARE_STATIONS).write_text(
        "name,station_id,name_short,lat,lon,id_region\n", encoding="latin-1"
    )


@pytest.fixture
def it_root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setattr(paths, "DATA_ROOT", tmp_path)
    aliases = tmp_path / "aliases.csv"
    aliases.write_text("name,code,display,lat,lon,note\n")
    monkeypatch.setattr(it, "ALIASES_CSV", aliases)
    ITALY.raw_dir.mkdir(parents=True, exist_ok=True)
    write_station_lists()
    return tmp_path


def build(days: list[date], fn_zero_days: list[date] | None = None):
    for d in days:
        it.extract_day(d)
    it.write_stations(days)
    con = duckdb.connect()
    it.create_tables(con)
    fn_zero = it.fn_zero_stations(con, fn_zero_days or [])
    result = it.stage_days(con, days, fn_zero)
    return con, result


def rows(con, trip_id):
    return con.execute(
        "SELECT station_id, sched_arr, sched_dep, act_arr, act_dep, arr_measured, dep_measured "
        "FROM stg_stops WHERE trip_id = ? ORDER BY stop_seq",
        [trip_id],
    ).fetchall()


D = date(2025, 6, 10)
T0 = utc(2025, 6, 10, 6, 0)  # 08:00 in Rome


def test_delays_markers_and_reconstruction(it_root):
    write_day(
        D,
        [
            train(
                101,
                [
                    stop("ALFA", 0, T0, "N", "2"),
                    stop("BETA", T0 + 300, T0 + 360, "3", "n.d."),
                    stop("GAMMA", T0 + 600, 0, "5", "N"),
                ],
            )
        ],
    )
    con, _ = build([D])
    (a, b, g) = rows(con, "REG 101")
    # Origin: no arrival event; departure scheduled + 2 min.
    assert a[1] is None and a[3] is None and a[2] == T0 and a[4] == T0 + 120 and a[6]
    # n.d. departure falls back to the timetable, flagged unmeasured; the arrival is measured.
    assert b[3] == T0 + 300 + 180 and b[5] and b[4] == T0 + 360 and not b[6]
    # Terminus: no departure event.
    assert g[2] is None and g[4] is None and g[3] == T0 + 600 + 300


def test_cancelled_stops_never_bridge(it_root):
    write_day(
        D,
        [
            train(
                202,
                [
                    stop("ALFA", 0, T0, "N", "0"),
                    stop("BETA", T0 + 300, T0 + 360, "S", "S"),
                    stop("GAMMA", T0 + 600, T0 + 660, "1", "1"),
                    stop("DELTA", T0 + 900, T0 + 960, "S", "S"),
                    stop("EPSILON", T0 + 1200, 0, "S", "N"),
                ],
                dl="Treno cancellato da BETA a BETA",
            )
        ],
    )
    con, res = build([D])
    assert res["calls_cancelled"] == 3
    legs = stops.build_legs(con, ITALY, D, D, ITALY.dim_station_parquet)
    got = con.execute("SELECT from_bpuic, to_bpuic FROM fct_legs ORDER BY t_dep").fetchall()
    # ALFA→BETA and BETA→GAMMA did not run, nor anything past GAMMA: no leg at all, and
    # above all no ALFA→GAMMA bridge.
    assert got == []
    assert legs["not_run"] == 4
    status = con.execute("SELECT status FROM it_trains").fetchone()[0]
    assert status == "part_cancelled"


def test_fully_cancelled_train_has_no_stops_but_is_counted(it_root):
    write_day(
        D,
        [
            train(303, [], rp="X", ra="X"),
            train(
                304,
                [stop("ALFA", 0, T0, "N", "0"), stop("BETA", T0 + 300, 0, "0", "N")],
                rp="X",
                ra="X",
            ),
            train(305, [stop("ALFA", 0, T0, "N", "0"), stop("BETA", T0 + 300, 0, "0", "N")]),
        ],
    )
    con, res = build([D])
    assert res["trains_cancelled"] == 2
    assert con.execute("SELECT DISTINCT trip_id FROM stg_stops").fetchall() == [("REG 305",)]
    assert con.execute("SELECT cancelled_trains FROM source_days").fetchone()[0] == 2


def test_past_midnight_and_dst_keep_the_service_day(it_root):
    # 2025-03-30: clocks go forward at 02:00 CET. A train departs 00:50 CET (23:50 UTC on the
    # 29th) and arrives 03:10 CEST (01:10 UTC): 80 minutes, not 140, on service day 03-30.
    day = date(2025, 3, 30)
    dep, arr = utc(2025, 3, 29, 23, 50), utc(2025, 3, 30, 1, 10)
    late = utc(2025, 3, 30, 21, 40)  # 23:40 CEST, arriving 00:30 on the 31st
    write_day(
        day,
        [
            train(401, [stop("ALFA", 0, dep, "N", "0"), stop("EPSILON", arr, 0, "0", "N")]),
            train(402, [stop("ALFA", 0, late, "N", "0"), stop("BETA", late + 3000, 0, "0", "N")]),
        ],
    )
    con, _ = build([day])
    stops.build_legs(con, ITALY, day, day, ITALY.dim_station_parquet)
    # Legs over an hour are split into hourly pieces (the shared split rule): sum them.
    got = dict(con.execute("SELECT trip_id, sum(dur) FROM fct_legs GROUP BY 1").fetchall())
    assert got["REG 401"] == 80 * 60
    assert got["REG 402"] == 50 * 60
    assert {d for (d,) in con.execute("SELECT DISTINCT service_day FROM stg_stops").fetchall()} == {
        day
    }


def test_inconsistent_arrival_falls_back_to_the_timetable(it_root):
    # Departs ALFA 4 min late, arrives BETA 1 min early on a 3-minute hop: reconstructed
    # arrival (T0+120) is before the reconstructed departure (T0+240).
    write_day(
        D,
        [train(501, [stop("ALFA", 0, T0, "N", "4"), stop("BETA", T0 + 180, 0, "-1", "N")])],
    )
    con, res = build([D])
    assert res["arrivals_inconsistent"] == 1
    stops.build_legs(con, ITALY, D, D, ITALY.dim_station_parquet)
    t_dep, dur, flags = con.execute("SELECT t_dep, dur, flags FROM fct_legs").fetchone()
    assert (t_dep, dur) == (T0, 180) and flags & 1


def test_extreme_delay_is_not_detected(it_root):
    write_day(
        D,
        [train(601, [stop("ALFA", 0, T0, "N", "900"), stop("BETA", T0 + 300, 0, "-45", "N")])],
    )
    con, res = build([D])
    (a, b) = rows(con, "REG 601")
    assert not a[6] and not b[5] and res["calls_extreme_delay"] == 2


def test_ferrovienord_zeros_before_the_fix_are_not_observations(it_root):
    old = date(2023, 6, 14)
    trains = [
        train(
            700 + i,
            [
                stop("ALFA", 0, T0 + i * 600, "N", "0"),
                stop("BETA", T0 + i * 600 + 300, 0, "0", "N"),
            ],
        )
        for i in range(110)
    ]
    write_day(old, trains)
    con, res = build([old], fn_zero_days=[old])
    assert res["calls_fn_zero_blind"] == 220
    assert (
        con.execute("SELECT bool_or(arr_measured OR dep_measured) FROM stg_stops").fetchone()[0]
        is False
    )


def test_duplicate_ids_and_the_duplicate_number_bug(it_root):
    ran = train(801, [stop("ALFA", 0, T0, "N", "0"), stop("BETA", T0 + 300, 0, "0", "N")])
    copy = dict(ran, fr=ran["fr"][:1])
    # The same number, cancelled, carrying the running train's exact times: not a train.
    ghost = dict(ran, _id="801-0-R0FNTUE=", rp="X", ra="X", fr=[])
    write_day(D, [ran, copy, ghost])
    con, res = build([D])
    assert res["duplicate_source_ids"] == 1 and res["trains_dup_number_bug"] == 1
    assert res["trains_cancelled"] == 1
    assert con.execute("SELECT cancelled_trains FROM source_days").fetchone()[0] == 0
    assert len(rows(con, "REG 801")) == 2


def test_frecce_category_and_number_change_label(it_root):
    write_day(
        D,
        [
            train(
                9519,
                [stop("ALFA", 0, T0, "N", "0"), stop("GAMMA", T0 + 600, 0, "0", "N")],
                cat="",
                sub="FR",
                cn="9520,GAMMA",
            )
        ],
    )
    con, _ = build([D])
    assert con.execute("SELECT trip_id, category, line FROM stg_stops LIMIT 1").fetchone() == (
        "FR 9519",
        "FR",
        "FR 9519/9520",
    )


def test_operational_points_are_dropped_and_joined_across(it_root):
    write_day(
        D,
        [
            train(
                901,
                [
                    stop("ALFA", 0, T0, "N", "0"),
                    stop("BIVIO PC NOWHERE", T0 + 120, T0 + 120, "0", "0"),
                    stop("BETA", T0 + 300, 0, "0", "N"),
                ],
            )
        ],
    )
    con, _ = build([D])
    stops.build_legs(con, ITALY, D, D, ITALY.dim_station_parquet)
    assert con.execute("SELECT from_bpuic, to_bpuic, dur FROM fct_legs").fetchall() == [(1, 2, 300)]


def test_unknown_field_fails_loudly(it_root):
    t = train(1001, [stop("ALFA", 0, T0, "N", "0"), stop("BETA", T0 + 300, 0, "0", "N")])
    t["zz"] = "new"
    write_day(D, [t])
    with pytest.raises(RuntimeError, match="unknown train keys"):
        it.extract_day(D)


def test_unknown_delay_marker_fails_loudly(it_root):
    write_day(
        D,
        [train(1002, [stop("ALFA", 0, T0, "N", "?"), stop("BETA", T0 + 300, 0, "0", "N")])],
    )
    with pytest.raises(RuntimeError, match="unknown delay marker"):
        it.extract_day(D)


def test_station_placed_by_its_neighbours_when_the_lists_disagree(it_root, monkeypatch):
    # GAMMA's registry point is 300 km off and OSM has no station of that name: the trains
    # between BETA and DELTA put it on the line, at the Figshare point.
    reg = json.loads((it.station_dir() / "elencoStazioni_0.json").read_text())
    for s in reg:
        if s["codStazione"] == "S00003":
            s["lat"] = 42.4
    (it.station_dir() / "elencoStazioni_0.json").write_text(json.dumps(reg))
    osm = json.loads((it.station_dir() / it.OSM_STATIONS).read_text())
    osm["elements"] = [e for e in osm["elements"] if e["tags"]["name"] != "Gamma"]
    (it.station_dir() / it.OSM_STATIONS).write_text(json.dumps(osm))
    (ITALY.raw_dir / it.FIGSHARE_STATIONS).write_text(
        "name,station_id,name_short,lat,lon,id_region\nGAMMA,S00003,Gamma,45.101,9.0,3\n",
        encoding="latin-1",
    )
    write_day(
        D,
        [
            train(
                1101,
                [
                    stop("BETA", 0, T0, "N", "0"),
                    stop("GAMMA", T0 + 300, T0 + 360, "0", "0"),
                    stop("DELTA", T0 + 600, 0, "0", "N"),
                ],
            )
        ],
    )
    for d in [D]:
        it.extract_day(d)
    summary = it.write_stations([D])
    lat = duckdb.sql(f"SELECT lat FROM '{ITALY.dim_station_parquet}' WHERE bpuic = 3").fetchone()[0]
    assert abs(lat - 45.101) < 1e-9
    assert summary["calls_by_status"].get("context_confirmed") == 1


def test_confirmed_point_contradicting_the_trains_is_demoted(it_root):
    # The index says OMEGA is S00099, and registry and OSM agree it is 400 km south; but its
    # trains run ALFA → OMEGA → BETA. It is not placed there.
    reg = json.loads((it.station_dir() / "elencoStazioni_0.json").read_text())
    reg.append(
        {
            "codStazione": "S00099",
            "lat": 41.4,
            "lon": 9.0,
            "esterno": False,
            "localita": {"nomeLungo": "OMEGA", "nomeBreve": "Omega"},
        }
    )
    (it.station_dir() / "elencoStazioni_0.json").write_text(json.dumps(reg))
    osm = json.loads((it.station_dir() / it.OSM_STATIONS).read_text())
    osm["elements"].append({"type": "node", "lat": 41.4, "lon": 9.0, "tags": {"name": "Omega"}})
    (it.station_dir() / it.OSM_STATIONS).write_text(json.dumps(osm))
    write_day(
        D,
        [
            train(
                1201,
                [
                    stop("ALFA", 0, T0, "N", "0"),
                    stop("OMEGA", T0 + 120, T0 + 180, "0", "0"),
                    stop("BETA", T0 + 300, 0, "0", "N"),
                ],
            )
        ],
    )
    it.extract_day(D)
    summary = it.write_stations([D])
    assert summary["calls_by_status"].get("context_conflict") == 1


def test_station_ids():
    assert it.station_id("S01700") == 1700
    assert it.station_id("F00001") == 1_000_001
    assert it.station_id("X00004") == 3_000_004
    assert it.normalize("CANTU`- CERMENATE") == "CANTU'-CERMENATE"
    assert it.squash("Città del Ragazzo") == it.squash("CITTA' DEL RAGAZZO")
