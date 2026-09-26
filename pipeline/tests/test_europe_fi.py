"""The Finnish adapter and the European dataset contract, on hand-written Digitraffic days.

Each fixture train encodes one rule from fi.py's docstring. The shape of a train is the real
API's: row 0 is the origin DEPARTURE, then ARRIVAL/DEPARTURE pairs, then the final ARRIVAL.
"""

import gzip
import importlib.util
import json
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pytest

from berg_pipeline import paths
from berg_pipeline.constants import FLAG_SCHEDULED_FALLBACK
from berg_pipeline.europe import catalog, fi, stops
from berg_pipeline.europe.config import FINLAND

DAY = date(2024, 3, 12)
STATIONS = [
    # (uic, short, name, lon, lat, country)
    (1, "HKI", "Helsinki", 24.941, 60.172, "FI"),
    (10, "PSL", "Pasila", 24.933, 60.199, "FI"),
    (18, "TKL", "Tikkurila", 25.043, 60.292, "FI"),
    (160, "TPE", "Tampere", 23.773, 61.498, "FI"),
    (1000, "AHV", "Ahvenus", 22.498, 61.292, "FI"),
    (1000, "PRK", "Petroskoi", 32.177, 62.117, "RU"),
    (99, "XXX", "Timing point", 24.95, 60.25, "FI"),
]


def ts(hh: int, mm: int, ss: int = 0) -> str:
    return f"{DAY.isoformat()}T{hh:02d}:{mm:02d}:{ss:02d}.000Z"


def row(kind, uic, sched, actual=None, stopping=True, commercial=True, cancelled=False, cc="FI"):
    out = {
        "type": kind,
        "cancelled": cancelled,
        "scheduledTime": sched,
        "trainStopping": stopping,
        "stationUICCode": uic,
        "countryCode": cc,
    }
    if stopping:
        out["commercialStop"] = commercial
    if actual:
        out["actualTime"] = actual
    return out


def train(number, rows, category="Long-distance", kind="IC", line=None, cancelled=False):
    return {
        "departureDate": DAY.isoformat(),
        "trainNumber": number,
        "trainType": kind,
        "trainCategory": category,
        "commuterLineID": line or "",
        "operatorShortCode": "vr",
        "cancelled": cancelled,
        "version": 1,
        "timeTableRows": rows,
    }


TRAINS = [
    # A measured IC HKI → PSL → (passes timing point 99) → TPE. Two legs, 99 dropped.
    train(27, [
        row("DEPARTURE", 1, ts(6, 0), ts(6, 1)),
        row("ARRIVAL", 10, ts(6, 5), ts(6, 6)),
        row("DEPARTURE", 10, ts(6, 6), ts(6, 7)),
        row("ARRIVAL", 99, ts(6, 20), ts(6, 21), stopping=False),
        row("DEPARTURE", 99, ts(6, 20), ts(6, 21), stopping=False),
        row("ARRIVAL", 160, ts(7, 40), ts(7, 42)),
    ]),
    # Commuter with an unmeasured arrival: its one leg falls back to the timetable, whole.
    train(9001, [
        row("DEPARTURE", 1, ts(8, 0), ts(8, 2)),
        row("ARRIVAL", 18, ts(8, 15)),
    ], category="Commuter", kind="HL", line="I"),
    # Cargo never reaches the facts.
    train(3000, [
        row("DEPARTURE", 1, ts(9, 0), ts(9, 0)),
        row("ARRIVAL", 160, ts(11, 0), ts(11, 0)),
    ], category="Cargo", kind="T"),
    # A cancelled train is dropped whole.
    train(28, [
        row("DEPARTURE", 1, ts(10, 0)),
        row("ARRIVAL", 160, ts(11, 40)),
    ], cancelled=True),
    # A stop cancelled mid-route: PSL's departure is gone, so PSL → TPE never ran.
    train(29, [
        row("DEPARTURE", 1, ts(12, 0), ts(12, 0)),
        row("ARRIVAL", 10, ts(12, 5), ts(12, 5)),
        row("DEPARTURE", 10, ts(12, 6), cancelled=True),
        row("ARRIVAL", 160, ts(13, 40), cancelled=True),
    ]),
    # A malformed train (two departures in a row) is excluded whole, never guessed at.
    train(31, [
        row("DEPARTURE", 1, ts(14, 0), ts(14, 0)),
        row("DEPARTURE", 10, ts(14, 5), ts(14, 5)),
        row("ARRIVAL", 160, ts(15, 40), ts(15, 40)),
    ]),
    # Station 1000 exists in two countries; this one is the Finnish Ahvenus.
    train(35, [
        row("DEPARTURE", 160, ts(16, 0), ts(16, 0)),
        row("ARRIVAL", 1000, ts(17, 0), ts(17, 0)),
    ], kind="H"),
]


@pytest.fixture
def fi_root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setattr(paths, "DATA_ROOT", tmp_path)
    raw = fi.raw_path(DAY)
    raw.parent.mkdir(parents=True)
    with gzip.open(raw, "wt") as fh:
        json.dump(TRAINS, fh)
    return tmp_path


@pytest.fixture
def dim(tmp_path: Path) -> Path:
    out = tmp_path / "dim_station.parquet"
    fi.write_dim_station(
        [
            {"stationUICCode": u, "stationShortCode": c, "stationName": n, "longitude": lo,
             "latitude": la, "passengerTraffic": True, "countryCode": cc}
            for u, c, n, lo, la, cc in STATIONS
        ],
        out,
    )
    return out


def built(fi_root, dim):
    con = duckdb.connect()
    staged = fi.stage_days(con, [DAY])
    legs = stops.build_legs(con, FINLAND, DAY, DAY, dim)
    return con, staged, legs


def epoch(hh, mm, ss=0) -> int:
    return int(datetime(DAY.year, DAY.month, DAY.day, hh, mm, ss, tzinfo=timezone.utc).timestamp())


def test_stage_keeps_passenger_commercial_stops_only(fi_root, dim):
    con, staged, _ = built(fi_root, dim)
    assert staged["trains_passenger"] == 6
    assert staged["trains_cancelled"] == 1
    assert staged["trains_malformed"] == 1
    trips = {t for (t,) in con.execute("SELECT DISTINCT trip_id FROM stg_stops").fetchall()}
    assert trips == {"IC 27", "HL 9001", "IC 29", "H 35"}
    stations_27 = [s for (s,) in con.execute(
        "SELECT station_id FROM stg_stops WHERE trip_id = 'IC 27' ORDER BY stop_seq").fetchall()]
    assert stations_27 == [1, 10, 160]  # timing point 99 dropped, source order kept


def test_times_are_utc_epochs_and_one_clock_per_leg(fi_root, dim):
    con, _, _ = built(fi_root, dim)
    legs = con.execute("""
        SELECT trip_id, from_bpuic, to_bpuic, t_dep, dur, delay, flags
        FROM fct_legs ORDER BY t_dep""").fetchall()
    ic = [leg for leg in legs if leg[0] == "IC 27"]
    # Measured both ends: actual departure 06:01, actual arrival 06:06, delay 60 s.
    assert ic[0][1:6] == (1, 10, epoch(6, 1), 300, 60)
    assert ic[0][6] & FLAG_SCHEDULED_FALLBACK == 0
    commuter = [leg for leg in legs if leg[0] == "HL 9001"]
    # Unmeasured arrival → the whole leg uses the timetable (15 min), but the measured
    # departure delay survives.
    assert len(commuter) == 1
    assert commuter[0][3:6] == (epoch(8, 0), 900, 120)
    assert commuter[0][6] & FLAG_SCHEDULED_FALLBACK


def test_cancelled_rows_never_become_movement(fi_root, dim):
    """IC 29 lost PSL's departure and TPE's arrival: TPE has no live row left, so the train
    terminates at PSL. Nothing is invented for the cancelled part and nothing is quarantined."""
    con, _, legs = built(fi_root, dim)
    ran = con.execute("SELECT from_bpuic, to_bpuic FROM fct_legs WHERE trip_id = 'IC 29'").fetchall()
    assert ran == [(1, 10)]
    assert "missing_time" not in legs


def test_uic_codes_are_scoped_by_country(fi_root, dim):
    assert fi.local_station_id("FI", 1000) == 1000
    assert fi.local_station_id("RU", 1000) != 1000
    sql = duckdb.sql(
        f"SELECT {fi._local_station_sql(repr('RU'), '1000')}, {fi._local_station_sql(repr('SE'), '7')}"
    ).fetchone()
    assert sql == (fi.local_station_id("RU", 1000), fi.local_station_id("SE", 7))
    con, _, _ = built(fi_root, dim)
    assert con.execute("SELECT to_bpuic FROM fct_legs WHERE trip_id = 'H 35'").fetchone() == (1000,)


def test_exports_the_shared_wire_contract(fi_root, dim, tmp_path):
    from berg_pipeline import ingest

    con, _, _ = built(fi_root, dim)
    out = tmp_path / "legs.parquet"
    # IC 27's 95-minute PSL → TPE leg is split into two hourly sub-legs: 3 + 1 + 1 + 1.
    assert ingest.export_day(con, DAY, out)["rows"] == 6
    cols = duckdb.sql(f"DESCRIBE SELECT * FROM '{out.as_posix()}'").fetchall()
    assert [c[0] for c in cols] == ["route_id", "journey_id", "t_dep", "dur", "type", "delay", "flags"]
    journeys = tmp_path / "journeys.parquet"
    ingest.export_journeys_day(con, DAY, journeys)
    lines = dict(duckdb.sql(f"SELECT trip_id, line FROM '{journeys.as_posix()}'").fetchall())
    assert lines["HL 9001"] == "I" and lines["IC 27"] == "IC 27"


def test_manifest_advertises_gaps_as_missing_coverage(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "DATA_ROOT", tmp_path)
    day_file = FINLAND.publish_root / "legs" / "2023" / "01" / "02.parquet"
    day_file.parent.mkdir(parents=True)
    duckdb.sql(f"""
        COPY (SELECT 1::UINTEGER AS route_id, 0::USMALLINT AS journey_id, 1::UINTEGER AS t_dep,
                     1::USMALLINT AS dur, 1::UTINYINT AS type, 0::SMALLINT AS delay,
                     0::UTINYINT AS flags)
        TO '{day_file.as_posix()}' (FORMAT PARQUET)""")
    manifest = catalog.build_dataset_manifest(FINLAND)
    assert manifest["start"] == "2023-01-01" and manifest["end"] == FINLAND.coverage_end
    assert "2023-01-01" in manifest["missing_days"]
    assert "2023-01-02" not in manifest["missing_days"]
    assert manifest["time_semantics"] == "observed"
    assert manifest["license"] == "CC BY 4.0"


def test_catalog_keeps_switzerland_at_the_root_and_carries_other_entries():
    live = {"datasets": {"ch": {"path": "stale"}, "nl": {"path": "datasets/nl"}}}
    cat = catalog.build_catalog(
        {"fi": {"start": "2023-01-01", "end": "2025-12-31"}}, existing=live
    )
    assert cat["datasets"]["ch"]["path"] == ""
    assert cat["datasets"]["ch"]["leg_schema_version"] == 3
    assert cat["datasets"]["nl"] == {"path": "datasets/nl"}
    assert cat["datasets"]["fi"]["path"] == "datasets/fi"
    assert cat["datasets"]["fi"]["timezone"] == "Europe/Helsinki"


SYNC = Path(__file__).parents[1] / "scripts" / "sync_dataset.py"
_spec = importlib.util.spec_from_file_location("sync_dataset", SYNC)
assert _spec is not None and _spec.loader is not None
sync_dataset = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sync_dataset)


def test_budget_gate_refuses_over_allocation_and_over_bucket():
    remote = {"legs/2020/01/01.parquet": {"size": 8_900_000_000},
              "datasets/fi/legs/2023/01/01.parquet": {"size": 50}}
    report = sync_dataset.budget(FINLAND, 90_000_000, remote)
    # The dataset's old bytes are replaced, not added.
    assert report["bucket_bytes_projected"] == 8_990_000_000
    with pytest.raises(RuntimeError, match="allocation"):
        sync_dataset.budget(FINLAND, FINLAND.storage_cap_bytes + 1, remote)
    with pytest.raises(RuntimeError, match="budget"):
        sync_dataset.budget(FINLAND, 110_000_000, remote)


def test_thin_day_passes_only_when_the_source_cancelled_it():
    con = duckdb.connect()
    stops.create_tables(con)
    con.execute("INSERT INTO source_days VALUES ('2023-03-21', 1121, 1120), ('2023-03-22', 1100, 20)")
    explained, unexplained = stops.thin_day_verdicts(
        con, {date(2023, 3, 21): 8, date(2023, 3, 22): 40, date(2023, 3, 23): 13_000}, 2_000
    )
    assert [d["day"] for d in explained] == ["2023-03-21"]
    assert unexplained == [("2023-03-22", 40)]  # a collapsed ingest still fails the build
