"""The click-detail sidecar contract."""

from datetime import date

import duckdb
import pytest

from berg_pipeline import ingest

# 2018-05-03 00:00 UTC. Legs are placed relative to this.
DAY = date(2018, 5, 3)
T0 = int((DAY - date(1970, 1, 1)).total_seconds())


@pytest.fixture
def con():
    c = duckdb.connect()
    ingest.create_tables(c)
    return c


def _leg(c, t_dep, trip_id="85:11:1", route_id=1, service_day=DAY, from_b=1, to_b=2):
    c.execute(
        """INSERT INTO fct_legs
           (service_day, trip_id, route_id, from_bpuic, to_bpuic, t_dep, dur, type_id, delay, flags)
           VALUES (?, ?, ?, ?, ?, ?, 300, 1, 0, 0)""",
        [service_day, trip_id, route_id, from_b, to_b, t_dep],
    )


def test_sidecar_maps_every_leg_to_one_journey(con, tmp_path):
    for i, t in enumerate([T0 + 300, T0 + 100, T0 + 200]):
        _leg(con, t, trip_id=f"85:11:{i}", route_id=i + 1)

    legs = tmp_path / "legs.parquet"
    journeys = tmp_path / "journeys.parquet"
    ingest.export_day(con, DAY, legs)
    stats = ingest.export_journeys_day(con, DAY, journeys)

    assert stats["rows"] == 3
    paired = duckdb.sql(f"""
        SELECT l.t_dep, l.journey_id, j.trip_id
        FROM read_parquet('{legs.as_posix()}') l
        JOIN read_parquet('{journeys.as_posix()}') j USING (journey_id)
        ORDER BY l.t_dep""").fetchall()
    assert paired == [
        (T0 + 100, 1, "85:11:1"),
        (T0 + 200, 2, "85:11:2"),
        (T0 + 300, 0, "85:11:0"),
    ]


def test_parallel_departures_have_unambiguous_journey_ids(con, tmp_path):
    """The old (t_dep, route_id) key collided for parallel trains on the same pair."""
    _leg(con, T0 + 100, trip_id="85:11:AAA", route_id=7)
    _leg(con, T0 + 100, trip_id="85:11:BBB", route_id=7)
    legs = tmp_path / "l.parquet"
    journeys = tmp_path / "j.parquet"
    ingest.export_day(con, DAY, legs)
    ingest.export_journeys_day(con, DAY, journeys)

    got = duckdb.sql(f"""
        SELECT l.t_dep, l.route_id, l.journey_id, j.trip_id
        FROM read_parquet('{legs.as_posix()}') l
        JOIN read_parquet('{journeys.as_posix()}') j USING (journey_id)
        ORDER BY journey_id""").fetchall()
    assert got == [
        (T0 + 100, 7, 0, "85:11:AAA"),
        (T0 + 100, 7, 1, "85:11:BBB"),
    ]


def test_all_legs_of_a_trip_share_one_journey_id(con, tmp_path):
    _leg(con, T0 + 100, trip_id="85:11:AAA", route_id=7)
    _leg(con, T0 + 400, trip_id="85:11:AAA", route_id=8, from_b=2, to_b=3)
    legs = tmp_path / "l.parquet"
    journeys = tmp_path / "j.parquet"
    ingest.export_day(con, DAY, legs)
    stats = ingest.export_journeys_day(con, DAY, journeys)

    assert duckdb.sql(
        f"SELECT DISTINCT journey_id FROM read_parquet('{legs.as_posix()}')"
    ).fetchall() == [(0,)]
    assert stats["rows"] == 1


def test_trip_id_alone_would_merge_two_service_days(con, tmp_path):
    """Why service_day is carried: (trip_id, service_day) is the journey key, trip_id isn't.

    A 00:30 departure belongs to the previous service day, so one file holds both.
    """
    _leg(con, T0 + 1800, trip_id="85:11:1", service_day=date(2018, 5, 2))  # 00:30, late service
    _leg(con, T0 + 40000, trip_id="85:11:1", service_day=DAY)  # same id, next day's run
    out = tmp_path / "j.parquet"
    ingest.export_journeys_day(con, DAY, out)

    rows = duckdb.sql(f"""
        SELECT journey_id, service_day FROM read_parquet('{out.as_posix()}')
        WHERE trip_id = '85:11:1' ORDER BY service_day""").fetchall()
    assert rows == [(0, date(2018, 5, 2)), (1, DAY)]


def test_day_boundary_matches_legs(con, tmp_path):
    """Departure-day keyed, [lo, hi) — a leg one second past midnight is the next file's."""
    _leg(con, T0 - 1)
    _leg(con, T0)
    _leg(con, T0 + 86400)
    out = tmp_path / "j.parquet"
    assert ingest.export_journeys_day(con, DAY, out)["rows"] == 1


def test_no_departures_writes_nothing(con, tmp_path):
    """The archive's 29 holes are expected-absent, not failures."""
    out = tmp_path / "j.parquet"
    out.write_bytes(b"stale")
    assert ingest.export_journeys_day(con, DAY, out) == {"rows": 0, "bytes": 0}
    assert not out.exists()


def test_route_pairs_json_maps_id_to_both_ends(con, tmp_path):
    con.execute("INSERT INTO station_pairs VALUES (8507000, 8507100, 1), (8501120, 8501008, 2)")
    out = tmp_path / "route_pairs.json"
    stats = ingest.export_route_pairs(con, out)

    import json

    assert stats["pairs"] == 2
    assert json.loads(out.read_text()) == {"1": [8507000, 8507100], "2": [8501120, 8501008]}
