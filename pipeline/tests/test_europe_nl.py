"""The Dutch adapter on a hand-written archive month.

Each fixture service encodes one rule from nl.py's docstring. Rows have the real archive's
columns and shapes: RFC 3339 times with the local offset, whole-minute delays, and
per-side cancellation flags that are empty where no arrival or departure was scheduled.
"""

import csv
import gzip
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pytest

from berg_pipeline import ingest, paths
from berg_pipeline.europe import catalog, nl, stops
from berg_pipeline.europe.config import NETHERLANDS

DAY = date(2024, 3, 12)  # CET, UTC+1
STATIONS = [
    # (code, uic, name, country, lat, lon)
    ("ASD", 8400058, "Amsterdam Centraal", "NL", 52.3789, 4.9003),
    ("UT", 8400621, "Utrecht Centraal", "NL", 52.0894, 5.1100),
    ("HT", 8400319, "'s-Hertogenbosch", "NL", 51.6905, 5.2936),
    ("EHV", 8400206, "Eindhoven Centraal", "NL", 51.4433, 5.4814),
    ("VL", 8400644, "Venlo", "NL", 51.3653, 6.1718),
    ("KALD", 8015199, "Kaldenkirchen", "D", 51.3190, 6.1970),
]

_sid = iter(range(1000, 2000))
_rid = iter(range(50_000, 60_000))


def t(hh: int, mm: int) -> str:
    return f"{DAY.isoformat()}T{hh:02d}:{mm:02d}:00+01:00"


def stop(code, arr=None, dep=None, arr_delay=0, dep_delay=0, arr_cx=False, dep_cx=False):
    return (code, arr, dep, arr_delay, dep_delay, arr_cx, dep_cx)


def service(number, stops_, kind="Intercity", cancelled=False, sid=None, numbers=None):
    sid = sid if sid is not None else next(_sid)
    rows = []
    for i, (code, arr, dep, ad, dd, acx, dcx) in enumerate(stops_):
        rows.append([
            sid, DAY.isoformat(), kind, "NS", (numbers or {}).get(i, number),
            str(cancelled).lower(), "false", 0, next(_rid), code, code,
            arr or "", ad if arr else "", str(acx).lower() if arr else "",
            dep or "", dd if dep else "", str(dcx).lower() if dep else "",
            "false", "", "",
        ])
    return rows


SERVICES = [
    # IC 800 ASD → UT → HT with delays: act = scheduled + delay minutes.
    service(800, [stop("ASD", dep=t(10, 0), dep_delay=2),
                  stop("UT", arr=t(10, 27), dep=t(10, 30), arr_delay=3, dep_delay=3),
                  stop("HT", arr=t(10, 58), arr_delay=1)]),
    # Replacement bus never reaches the facts.
    service(900, [stop("ASD", dep=t(11, 0)), stop("UT", arr=t(11, 40))],
            kind="Stopbus ipv trein"),
    # A completely cancelled service is dropped whole.
    service(801, [stop("ASD", dep=t(12, 0), dep_cx=True), stop("UT", arr=t(12, 27), arr_cx=True)],
            cancelled=True),
    # Cut short at UT and resumed at EHV: UT → HT → EHV never ran, EHV → VL did.
    service(802, [stop("ASD", dep=t(13, 0)),
                  stop("UT", arr=t(13, 27), dep=t(13, 30), dep_cx=True),
                  stop("HT", arr=t(13, 58), dep=t(14, 0), arr_cx=True, dep_cx=True),
                  stop("EHV", arr=t(14, 20), dep=t(14, 22), arr_cx=True),
                  stop("VL", arr=t(14, 50))]),
    # A diversion appended after the terminus, out of time order: excluded whole.
    service(803, [stop("ASD", dep=t(15, 0)), stop("HT", arr=t(15, 58)), stop("UT", arr=t(15, 27))]),
    # A cross-border Sprinter: VL → KALD is clipped as outside_country, not quarantined.
    service(6000, [stop("EHV", dep=t(16, 0)), stop("VL", arr=t(16, 30), dep=t(16, 32)),
                   stop("KALD", arr=t(16, 40))], kind="Sprinter"),
    # One train, three records: IC 2452 renumbered 3563 at UT (sid 3000), the 3563-only
    # portion (sid 3001, a subset) and a re-issue under 302452 (sid 3002, equal size, later).
    # Only sid 3000 survives, labelled by its first number.
    service(2452, [stop("ASD", dep=t(17, 0)), stop("UT", arr=t(17, 27), dep=t(17, 30)),
                   stop("EHV", arr=t(18, 20))], sid=3000, numbers={1: 3563, 2: 3563}),
    service(3563, [stop("UT", dep=t(17, 30)), stop("EHV", arr=t(18, 20))], sid=3001),
    service(302452, [stop("ASD", dep=t(17, 0)), stop("UT", arr=t(17, 27), dep=t(17, 30)),
                     stop("EHV", arr=t(18, 20))], sid=3002),
]


@pytest.fixture
def nl_root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setattr(paths, "DATA_ROOT", tmp_path)
    raw = nl.raw_path(f"{DAY:%Y-%m}")
    raw.parent.mkdir(parents=True)
    with gzip.open(raw, "wt", newline="") as fh:
        fh.write(nl.HEADER + "\n")
        writer = csv.writer(fh)
        for rows in SERVICES:
            writer.writerows(rows)
    with open(nl.stations_path(), "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["id", "code", "uic", "name_short", "name_medium", "name_long", "slug",
                         "country", "type", "geo_lat", "geo_lng"])
        for i, (code, uic, name, country, lat, lon) in enumerate(STATIONS):
            writer.writerow([i, code, uic, name, name, name, code.lower(), country, "x", lat, lon])
    return tmp_path


def built(nl_root):
    dim = NETHERLANDS.dim_station_parquet
    nl.write_dim_station(nl.stations_path(), dim)
    con = duckdb.connect()
    staged = nl.stage_days(con, [DAY], dim)
    legs = stops.build_legs(con, NETHERLANDS, DAY, DAY, dim)
    return con, staged, legs


def epoch(hh, mm) -> int:
    return int(datetime(DAY.year, DAY.month, DAY.day, hh - 1, mm, tzinfo=timezone.utc).timestamp())


def trips(con) -> set[str]:
    return {r[0] for r in con.execute("SELECT DISTINCT trip_id FROM fct_legs").fetchall()}


def test_trains_only_and_cancelled_services_dropped(nl_root):
    con, staged, _ = built(nl_root)
    assert staged["services_train"] == 8  # the bus is gone before counting
    assert staged["services_cancelled"] == 1
    assert "IC 801" not in trips(con) and "IC 900" not in trips(con)


def test_times_are_scheduled_plus_delay_with_the_offset_honoured(nl_root):
    con, _, _ = built(nl_root)
    legs = con.execute("""
        SELECT from_bpuic, to_bpuic, t_dep, dur, delay, flags FROM fct_legs
        WHERE trip_id = 'IC 800' ORDER BY t_dep""").fetchall()
    # ASD dep 10:00+2 → UT arr 10:27+3 = 28 min; 10:00 CET is 09:00 UTC.
    assert legs[0] == (8400058, 8400621, epoch(10, 2), 28 * 60, 120, 0)
    assert legs[1][2:5] == (epoch(10, 33), 26 * 60, 180)


def test_a_cancelled_middle_never_becomes_a_bridging_leg(nl_root):
    con, _, legs = built(nl_root)
    ran = con.execute(
        "SELECT from_bpuic, to_bpuic FROM fct_legs WHERE trip_id = 'IC 802' ORDER BY t_dep"
    ).fetchall()
    assert ran == [(8400058, 8400621), (8400206, 8400644)]  # ASD→UT, EHV→VL
    assert legs["not_run"] == 1  # UT → EHV (HT is gone: both its sides were cancelled)
    assert "missing_time" not in legs


def test_diversions_are_excluded_whole(nl_root):
    con, staged, _ = built(nl_root)
    assert staged["services_malformed"] == 1
    assert "IC 803" not in trips(con)


def test_foreign_endpoints_are_clipped_not_quarantined(nl_root):
    con, _, legs = built(nl_root)
    assert con.execute(
        "SELECT from_bpuic, to_bpuic FROM fct_legs WHERE trip_id = 'SPR 6000'"
    ).fetchall() == [(8400206, 8400644)]
    assert legs["outside_country"] == 1
    assert con.execute("SELECT count(*) FROM quarantine_legs").fetchone() == (0,)


def test_duplicate_records_of_one_train_collapse_to_the_first(nl_root):
    con, staged, _ = built(nl_root)
    assert staged["services_duplicate"] == 2
    assert {t for t in trips(con) if "2452" in t or "3563" in t} == {"IC 2452"}
    assert con.execute("SELECT count(*) FROM fct_legs WHERE trip_id = 'IC 2452'").fetchone() == (2,)


def test_exports_and_manifest(nl_root):
    con, _, _ = built(nl_root)
    legs_dir = NETHERLANDS.publish_root / "legs"
    assert ingest.export_day(con, DAY, legs_dir / f"{DAY:%Y/%m/%d}.parquet")["rows"] > 0
    manifest = catalog.build_dataset_manifest(NETHERLANDS)
    assert manifest["time_semantics"] == "delay_only"
    assert manifest["timestamp_precision_s"] == 60
    assert DAY.isoformat() in manifest["days"]
    assert "2024-03-11" in manifest["missing_days"]  # a gap is missing coverage, not no trains
