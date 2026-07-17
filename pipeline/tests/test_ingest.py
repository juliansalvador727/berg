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
    line="IC 8",
    umlauf="",
    extra="false",
    passthrough="false",
):
    return ";".join(
        [
            day, trip, "85:11", "SBB", "SBB AG", product, "", line, umlauf, cat, extra,
            cancelled, str(bpuic), f"S{bpuic}",
            an or "", an_prog or "", an_status, ab or "", ab_prog or "", ab_status, passthrough,
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
    fractions = con.execute("SELECT route_start, route_end FROM fct_legs ORDER BY t_dep").fetchall()
    assert fractions == [(0, 102), (102, 204), (204, 255)]

    out = tmp_path / "split.parquet"
    ingest.export_day(con, date(2018, 5, 3), out)
    packed = duckdb.sql(f"SELECT route_id, flags FROM read_parquet('{out.as_posix()}')").fetchall()
    base_route_id = con.execute("SELECT min(route_id) FROM fct_legs").fetchone()[0]
    assert [(r & 0xFFFF, (r >> 16) & 0xFF, r >> 24, flags) for r, flags in packed] == [
        (base_route_id, 0, 102, 6),
        (base_route_id, 102, 204, 6),
        (base_route_id, 204, 255, 6),
    ]


def test_leg_uses_one_clock_when_only_the_departure_is_measured(tmp_path, dim):
    """A leg's two ends must come from the same basis, or dur is not a duration.

    Departure measured 6 minutes late at 08:06; the next stop reports no arrival, so only its
    timetable 08:05 exists. Taking each end's best available time subtracts a scheduled arrival
    from an actual departure and yields dur = -60: the train arrives before it leaves, and gets
    quarantined as a source error it never was. Falling back to the timetable at BOTH ends gives
    the 5-minute booked hop, flagged scheduled — which is what the flag has always claimed.

    Archive-wide this is 33.6% of mixed legs going negative against 0.0% for legs on one clock,
    and ~6M more that stay positive and publish a duration built from two clocks.
    """
    con, _, stats = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:06:00", ab_status="REAL"),
            ev(bpuic=2, an="03.05.2018 08:05"),  # booked only — no AN_PROGNOSE, no status
        ],
    )
    assert "negative_duration" not in stats
    ls = legs_of(con)
    assert len(ls) == 1
    assert ls[0][4] == 300, "should be the booked 5 min, not 08:05 minus 08:06"
    assert ls[0][6] & 1, "a leg on timetable geometry must carry FLAG_SCHEDULED_FALLBACK"


def test_measured_delay_survives_a_scheduled_fallback_leg(tmp_path, dim):
    """Falling back for geometry must not throw away a delay we actually measured.

    The departure delay needs only this stop's own scheduled and actual times, so it is known
    even when the next stop never reported and the leg rides the timetable.
    """
    con, _, _ = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:06:00", ab_status="REAL"),
            ev(bpuic=2, an="03.05.2018 08:05"),
        ],
    )
    assert legs_of(con)[0][5] == 360


def test_absurd_duration_quarantined_before_the_split_amplifies_it(tmp_path, dim):
    """A broken timestamp must not become a million rows.

    This is 2025-09-10 in miniature: one ski shuttle arrived with a departure dated 1899, the
    126-year "leg" cleared every check the contract had, and the split rule expanded that single
    source row into 1,101,793 synthetic sub-legs. The split rule is not the bug — it did exactly
    what it was told — so the guard belongs upstream of it, where dur is still a duration and
    not yet a row multiplier.
    """
    con, _, stats = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.1899 08:00", ab_prog="03.05.1899 08:00:00", ab_status="REAL"),
            ev(bpuic=4, an="03.05.2018 10:30", an_prog="03.05.2018 10:30:00", an_status="REAL"),
        ],
    )
    assert stats.get("absurd_duration") == 1
    assert legs_of(con) == []  # not one sub-leg, let alone a million
    assert con.execute("SELECT reason FROM quarantine_legs").fetchone()[0] == "absurd_duration"


def test_long_but_plausible_leg_still_splits(tmp_path, dim):
    """The guard is a bound, not a ban: 2 h is where the real distribution lives."""
    con, _, stats = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
            ev(bpuic=4, an="03.05.2018 10:00", an_prog="03.05.2018 10:00:00", an_status="REAL"),
        ],
    )
    assert stats["ok"] == 1 and len(legs_of(con)) == 2
    assert "absurd_duration" not in stats


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
    out.write_bytes(b"stale")
    stats = ingest.export_day(con, date(2018, 5, 24), out)
    assert stats["rows"] == 0
    assert not out.exists()


def test_truncated_row_does_not_kill_the_month(tmp_path, dim):
    """The archive ships short rows — 2024-10-26.csv ends mid-line, 1 of 1.6M.

    Verbatim from the real file. Strict mode failed the entire month over this single bus.
    """
    truncated = (
        "26.10.2024;85:885:2381-2;85:885;VBSG;Verkehrsbetriebe der Stadt St.Gallen;BUS;"
        "85:885:2;2;201;B;false;false;8589606;;26.10.2024 15:30;26.10.2024 15:30"
    )
    con, stage, _ = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
            ev(bpuic=2, an="03.05.2018 08:30", an_prog="03.05.2018 08:30:00", an_status="REAL"),
            truncated,
        ],
    )
    # The month parses, the train survives, and the padded bus never reaches the facts.
    assert stage["rows_raw"] == 3
    assert stage["rows_train"] == 2
    assert len(legs_of(con)) == 1


def test_encoding_is_per_file_not_per_month(tmp_path, dim):
    """The archive mixes encodings INSIDE a month: 2018-11-01 is UTF-8, 2018-11-02 is latin-1.

    So there is no single encoding to pass to read_csv. Verified against the real files:
    latin-1 is not a catch-all either — DuckDB validates it and rejects the UTF-8 file.
    """
    utf8_csv = tmp_path / "2018-05-03.csv"
    latin_csv = tmp_path / "2018-05-04.csv"
    # 'Möhlin' as a station name — the real shape of the bytes that killed 2018-11.
    row_utf8 = ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL")
    row_latin = ev(day="04.05.2018", bpuic=2, an="04.05.2018 08:30",
                   an_prog="04.05.2018 08:30:00", an_status="REAL")  # fmt: skip
    utf8_csv.write_bytes(
        ("\n".join([HEADER, row_utf8.replace("S1", "Zürich")]) + "\n").encode("utf-8")
    )
    latin_csv.write_bytes(
        ("\n".join([HEADER, row_latin.replace("S2", "Möhlin")]) + "\n").encode("latin-1")
    )

    assert ingest.file_encoding(utf8_csv) == "utf-8"
    assert ingest.file_encoding(latin_csv) == "latin-1"

    con = duckdb.connect()
    split = ingest._stage_raw_view(con, [utf8_csv, latin_csv])
    assert split == {"latin-1": 1, "utf-8": 1}
    # Both files are readable through one view — neither encoding is dropped.
    assert con.execute("SELECT count(*) FROM _raw").fetchone()[0] == 2


def test_line_flows_from_csv_to_legs(tmp_path, dim):
    """LINIEN_TEXT is the line ('S3'), distinct from the category ('S').

    Staged greedily because the raw CSVs are deleted after ingest: a column not taken at this
    boundary costs a 1.27 TB re-download to add later.
    """
    con, _, _ = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, line="S3", cat="S",
               ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
            ev(bpuic=2, line="S3", cat="S",
               an="03.05.2018 08:30", an_prog="03.05.2018 08:30:00", an_status="REAL"),
        ],
    )  # fmt: skip
    assert con.execute("SELECT DISTINCT line FROM fct_legs").fetchall() == [("S3",)]


def test_blank_line_becomes_null_not_empty_string(tmp_path, dim):
    """Some runs carry no LINIEN_TEXT; '' would pollute the sidecar's dictionary."""
    con, _, _ = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, line="   ",
               ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
            ev(bpuic=2, line="",
               an="03.05.2018 08:30", an_prog="03.05.2018 08:30:00", an_status="REAL"),
        ],
    )  # fmt: skip
    assert con.execute("SELECT line FROM fct_legs").fetchall() == [(None,)]


def test_migrate_tables_is_idempotent_and_preserves_rows(tmp_path, dim):
    """An existing 185M-row database must GAIN the columns, never be rebuilt to get them."""
    con, _, _ = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
            ev(bpuic=2, an="03.05.2018 08:30", an_prog="03.05.2018 08:30:00", an_status="REAL"),
        ],
    )
    before = con.execute("SELECT count(*) FROM fct_legs").fetchone()[0]
    ingest.migrate_tables(con)
    ingest.migrate_tables(con)  # twice — ALTER ... IF NOT EXISTS must not fail
    assert con.execute("SELECT count(*) FROM fct_legs").fetchone()[0] == before
    cols = [d[0] for d in con.execute("SELECT * FROM stg_istdaten LIMIT 0").description]
    assert {"line", "umlauf_id", "is_extra", "is_passthrough"} <= set(cols)


def test_sidecar_carries_line_but_the_wire_does_not(tmp_path, dim):
    """line belongs to a journey, so it rides in journeys/ rather than every leg."""
    con, _, _ = run_month(
        tmp_path,
        dim,
        [
            ev(bpuic=1, line="IR 15",
               ab="03.05.2018 08:00", ab_prog="03.05.2018 08:00:00", ab_status="REAL"),
            ev(bpuic=2, line="IR 15",
               an="03.05.2018 08:30", an_prog="03.05.2018 08:30:00", an_status="REAL"),
        ],
    )  # fmt: skip
    out = tmp_path / "j.parquet"
    ingest.export_journeys_day(con, date(2018, 5, 3), out)
    assert duckdb.sql(f"SELECT DISTINCT line FROM read_parquet('{out.as_posix()}')").fetchall() == [
        ("IR 15",)
    ]

    legs = tmp_path / "l.parquet"
    ingest.export_day(con, date(2018, 5, 3), legs)
    wire = [
        d[0] for d in duckdb.sql(f"SELECT * FROM read_parquet('{legs.as_posix()}')").description
    ]
    assert "line" not in wire
    assert "journey_id" in wire


def test_departure_days_include_both_utc_boundaries():
    days = ingest.departure_days_for_month("2019-03")

    assert days[0] == date(2019, 2, 28)
    assert days[1] == date(2019, 3, 1)
    assert days[-2] == date(2019, 3, 31)
    assert days[-1] == date(2019, 4, 1)


def test_published_registries_seed_stable_wire_ids():
    con = duckdb.connect()
    stats = ingest.seed_registries(
        con,
        {"42": [8507000, 8507100]},
        {"7": "IC"},
    )

    assert stats == {"route_pairs_seeded": 1, "train_types_seeded": 1}
    assert con.execute("SELECT * FROM station_pairs").fetchall() == [(8507000, 8507100, 42)]
    assert con.execute("SELECT * FROM dim_train_type").fetchall() == [("IC", 7)]

    # Compatible re-seeding is idempotent; the next locally discovered ids append after these.
    assert ingest.seed_registries(con, {"42": [8507000, 8507100]}, {"7": "IC"}) == {
        "route_pairs_seeded": 0,
        "train_types_seeded": 0,
    }
