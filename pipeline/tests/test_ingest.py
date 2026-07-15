"""The ingest contract, exercised on synthetic CSVs small enough to reason about by hand.

Each test encodes one rule from docs/data-notes.md; if a rule ever stops holding against the
real archive, the fixture here is the spec of what the pipeline believed.
"""

from datetime import date
from pathlib import Path

import duckdb
import pytest

from berg_pipeline import ingest

HEADER = (
    "BETRIEBSTAG;FAHRT_BEZEICHNER;BETREIBER_ID;BETREIBER_ABK;BETREIBER_NAME;PRODUKT_ID;"
    "LINIEN_ID;LINIEN_TEXT;UMLAUF_ID;VERKEHRSMITTEL_TEXT;ZUSATZFAHRT_TF;FAELLT_AUS_TF;BPUIC;"
    "HALTESTELLEN_NAME;ANKUNFTSZEIT;AN_PROGNOSE;AN_PROGNOSE_STATUS;ABFAHRTSZEIT;AB_PROGNOSE;"
    "AB_PROGNOSE_STATUS;DURCHFAHRT_TF"
)

# Stations for the fixture dimension. 1..4 are inside the CH bbox, 9 is Milan-ish (outside),
# 5 exists but only from 2020 (validity miss for 2018 events).
STATIONS = {
    1: (8.54, 47.38),
    2: (8.31, 47.05),
    3: (7.44, 46.95),
    4: (6.63, 46.52),
    5: (9.36, 47.42),
    9: (9.19, 45.46),
}


@pytest.fixture
def dim(tmp_path: Path) -> Path:
    con = duckdb.connect()
    rows = []
    for bpuic, (lon, lat) in STATIONS.items():
        valid_from = "2020-01-01" if bpuic == 5 else "2016-01-01"
        rows.append(f"({bpuic}, 'S{bpuic}', {lon}, {lat}, DATE '{valid_from}', DATE '9999-12-31')")
    out = tmp_path / "dim_station.parquet"
    con.execute(f"""
        COPY (SELECT * FROM (VALUES {", ".join(rows)})
              t(bpuic, name, lon, lat, valid_from, valid_to))
        TO '{out.as_posix()}' (FORMAT PARQUET)""")
    return out


def ev(
    day="03.05.2018",
    trip="85:11:1",
    product="Zug",
    cat="IC",
    cancelled="false",
    bpuic=1,
    an=None,
    an_prog=None,
    an_status="",
    ab=None,
    ab_prog=None,
    ab_status="",
):
    return ";".join(
        [
            day, trip, "85:11", "SBB", "SBB AG", product, "", "", "", cat, "false",
            cancelled, str(bpuic), f"S{bpuic}",
            an or "", an_prog or "", an_status, ab or "", ab_prog or "", ab_status, "false",
        ]
    )  # fmt: skip


def run_month(tmp_path: Path, dim: Path, lines: list[str], month="2018-05"):
    csv = tmp_path / "2018-05-03.csv"
    csv.write_text("\n".join([HEADER, *lines]) + "\n")
    con = duckdb.connect()
    stage = ingest.stage_month(con, month, [csv])
    stats = ingest.build_legs(con, month, dim)
    return con, stage, stats


def legs_of(con):
    return con.execute(
        """SELECT trip_id, from_bpuic, to_bpuic, t_dep, dur, delay, flags
           FROM fct_legs ORDER BY trip_id, t_dep"""
    ).fetchall()


def test_measured_leg_uses_actual_times_and_utc(tmp_path, dim):
    # 08:00 CEST departure = 06:00 UTC. Actual dep 08:01:30 (delay 90 s), actual arr 08:31:00.
    con, _, stats = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:01:30", ab_status="REAL"),
            ev(bpuic=2, an="03.05.2018 08:30", an_prog="03.05.2018 08:31:00", an_status="REAL"),
        ],
    )
    (leg,) = legs_of(con)
    assert stats["ok"] == 1
    _, f, t, t_dep, dur, delay, flags = leg
    assert (f, t) == (1, 2)
    assert t_dep == 1525327290  # 2018-05-03 06:01:30 UTC
    assert dur == 1770  # 08:01:30 → 08:31:00
    assert delay == 90
    assert flags == 0


def test_geschaetzt_counts_as_measured(tmp_path, dim):
    # Pre-2018-05-07 the measured enum is GESCHAETZT; the set match must accept it.
    con, _, stats = run_month(
        tmp_path,
        dim,
        [
            ev(
                bpuic=1,
                ab="03.05.2018 08:00",
                ab_prog="03.05.2018 08:00:10",
                ab_status="GESCHAETZT",
            ),
            ev(
                bpuic=2,
                an="03.05.2018 08:30",
                an_prog="03.05.2018 08:30:05",
                an_status="GESCHAETZT",
            ),
        ],
    )
    (leg,) = legs_of(con)
    assert stats["ok"] == 1 and leg[6] == 0  # measured, no fallback flag
    summary = ingest.month_summary(con, "2018-05")
    assert summary["legs"] == 1 and summary["pct_measured"] == 100.0


def test_forecast_falls_back_to_scheduled_and_flags(tmp_path, dim):
    # PROGNOSE is a forecast, never an observation: use scheduled times, set bit0.
    con, _, _ = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:05:00", ab_status="PROGNOSE"),
            ev(bpuic=2, an="03.05.2018 08:30", an_prog="03.05.2018 08:39:00", an_status="PROGNOSE"),
        ],
    )
    (leg,) = legs_of(con)
    _, _, _, t_dep, dur, delay, flags = leg
    assert t_dep == 1525327200  # scheduled 08:00 CEST
    assert dur == 1800  # scheduled, not the forecast times
    assert delay == 0
    assert flags == 1


def test_stop_order_is_scheduled_not_actual(tmp_path, dim):
    # A delayed train's ACTUAL times go non-monotonic; scheduled order must win.
    con, _, _ = run_month(
        tmp_path,
        dim,
        [
            ev(
                bpuic=2,
                an="03.05.2018 08:30",
                an_prog="03.05.2018 09:00:00",
                an_status="REAL",
                ab="03.05.2018 08:31",
                ab_prog="03.05.2018 09:01:00",
                ab_status="REAL",
            ),  # fmt: skip
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
            ev(bpuic=3, an="03.05.2018 09:00", an_prog="03.05.2018 09:20:00", an_status="REAL"),
        ],
    )
    got = [(f, t) for _, f, t, *_ in legs_of(con)]
    assert got == [(1, 2), (2, 3)]


def test_negative_duration_quarantined(tmp_path, dim):
    con, _, stats = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:10:00", ab_status="REAL"),
            ev(bpuic=2, an="03.05.2018 08:05", an_prog="03.05.2018 08:04:00", an_status="REAL"),
        ],
    )
    assert stats.get("negative_duration") == 1
    assert legs_of(con) == []
    reason = con.execute("SELECT reason FROM quarantine_legs").fetchone()[0]
    assert reason == "negative_duration"


def test_split_rule(tmp_path, dim):
    # 2.5 h nonstop → three sub-legs (3600 + 3600 + 1800), all flagged synthetic.
    con, _, stats = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
            ev(bpuic=4, an="03.05.2018 10:30", an_prog="03.05.2018 10:30:00", an_status="REAL"),
        ],
    )
    ls = legs_of(con)
    assert stats["ok"] == 1 and len(ls) == 3
    assert [leg[4] for leg in ls] == [3600, 3600, 1800]
    assert all(leg[6] & 2 for leg in ls)
    assert ls[1][3] == ls[0][3] + 3600 and ls[2][3] == ls[1][3] + 3600


def test_bus_and_cancelled_filtered_at_stage(tmp_path, dim):
    _, stage, _ = run_month(
        tmp_path,
        dim,
        [
            ev(product="BUS", bpuic=1, ab="03.05.2018 08:00"),
            ev(product="Bus", bpuic=1, ab="03.05.2018 08:00"),
            ev(cancelled="true", bpuic=1, ab="03.05.2018 08:00"),
            ev(bpuic=1, ab="03.05.2018 08:00"),
        ],
    )
    assert stage["rows_staged"] == 1
    assert stage["rows_cancelled"] == 1


def test_foreign_clip_and_unmatched_station(tmp_path, dim):
    con, _, stats = run_month(
        tmp_path,
        dim,
        [
            # leg into Milan (station 9, outside bbox): clipped, not quarantined
            ev(
                trip="A",
                bpuic=1,
                ab="03.05.2018 08:00",
                ab_prog="03.05.2018 08:00:00",
                ab_status="REAL",
            ),
            ev(
                trip="A",
                bpuic=9,
                an="03.05.2018 09:00",
                an_prog="03.05.2018 09:00:00",
                an_status="REAL",
            ),
            # leg to station 5, which only exists in the dim from 2020: unmatched in 2018
            ev(
                trip="B",
                bpuic=1,
                ab="03.05.2018 08:00",
                ab_prog="03.05.2018 08:00:00",
                ab_status="REAL",
            ),
            ev(
                trip="B",
                bpuic=5,
                an="03.05.2018 08:20",
                an_prog="03.05.2018 08:20:00",
                an_status="REAL",
            ),
        ],
    )
    assert stats.get("outside_ch") == 1
    assert stats.get("unmatched_station") == 1
    assert legs_of(con) == []
    assert con.execute("SELECT count(*) FROM quarantine_legs").fetchone()[0] == 1


def test_route_and_type_ids_stable_across_reruns(tmp_path, dim):
    lines = [
        ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
        ev(bpuic=2, an="03.05.2018 08:30", an_prog="03.05.2018 08:30:00", an_status="REAL"),
    ]
    con, _, _ = run_month(tmp_path, dim, lines)
    before = con.execute("SELECT route_id, type_id FROM fct_legs").fetchone()
    csv = tmp_path / "2018-05-03.csv"
    ingest.stage_month(con, "2018-05", [csv])
    ingest.build_legs(con, "2018-05", dim)
    after = con.execute("SELECT route_id, type_id FROM fct_legs").fetchone()
    assert before == after
    assert con.execute("SELECT count(*) FROM fct_legs").fetchone()[0] == 1


def test_export_keyed_by_departure_day_utc(tmp_path, dim):
    # Departure 23:30 UTC on May 3 (01:30 local May 4): belongs to May 3's file, and the
    # night leg departing 00:30 UTC May 4 to May 4's — even though both are service day 03.05.
    con, _, _ = run_month(
        tmp_path,
        dim,
        [
            ev(
                trip="N",
                bpuic=1,
                ab="04.05.2018 01:30",
                ab_prog="04.05.2018 01:30:00",
                ab_status="REAL",
            ),
            ev(
                trip="N",
                bpuic=2,
                an="04.05.2018 02:00",
                an_prog="04.05.2018 02:00:00",
                an_status="REAL",
                ab="04.05.2018 02:30",
                ab_prog="04.05.2018 02:30:00",
                ab_status="REAL",
            ),  # fmt: skip
            ev(
                trip="N",
                bpuic=3,
                an="04.05.2018 03:00",
                an_prog="04.05.2018 03:00:00",
                an_status="REAL",
            ),
        ],
    )
    d3 = ingest.export_day(con, date(2018, 5, 3), tmp_path / "03.parquet")
    d4 = ingest.export_day(con, date(2018, 5, 4), tmp_path / "04.parquet")
    assert (d3["rows"], d4["rows"]) == (1, 1)
    t = duckdb.sql(f"SELECT t_dep FROM '{tmp_path / '03.parquet'}'").fetchone()[0]
    assert t == 1525390200  # 2018-05-03 23:30:00 UTC
    # wire types survive the round trip
    schema = dict(
        duckdb.sql(f"SELECT name, type FROM parquet_schema('{tmp_path / '04.parquet'}')").fetchall()
    )
    assert schema["dur"] == "INT32"  # physical; logical uint16 checked via reading range
    assert d3["bytes"] > 0


def test_missing_day_exports_nothing(tmp_path, dim):
    con, _, _ = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
            ev(bpuic=2, an="03.05.2018 08:30", an_prog="03.05.2018 08:30:00", an_status="REAL"),
        ],
    )
    out = tmp_path / "24.parquet"
    stats = ingest.export_day(con, date(2018, 5, 24), out)
    assert stats["rows"] == 0
    assert not out.exists()
