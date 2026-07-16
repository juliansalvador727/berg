"""The click-detail sidecar contract.

The invariant these pin down: journeys/ must line up with legs/ exactly — same rows, same
order, same day boundary — because it exists to answer questions *about* those legs.
"""

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


def test_sidecar_matches_legs_row_for_row(con, tmp_path):
    """Same WHERE and ORDER BY as export_day, so row N here is leg N there."""
    for i, t in enumerate([T0 + 300, T0 + 100, T0 + 200]):
        _leg(con, t, trip_id=f"85:11:{i}", route_id=i + 1)

    legs = tmp_path / "legs.parquet"
    journeys = tmp_path / "journeys.parquet"
    ingest.export_day(con, DAY, legs)
    stats = ingest.export_journeys_day(con, DAY, journeys)

    assert stats["rows"] == 3
    paired = duckdb.sql(f"""
        SELECT l.t_dep, l.route_id, j.t_dep, j.route_id
        FROM (SELECT *, row_number() OVER () AS rn FROM read_parquet('{legs.as_posix()}')) l
        JOIN (SELECT *, row_number() OVER () AS rn FROM read_parquet('{journeys.as_posix()}')) j
          USING (rn)""").fetchall()
    assert [(a, b) for a, b, _, _ in paired] == [(c, d) for _, _, c, d in paired]
    # and sorted by t_dep, which is what makes the lookup prune
    assert [r[0] for r in paired] == [T0 + 100, T0 + 200, T0 + 300]


def test_lookup_by_t_dep_and_route_id_finds_the_trip(con, tmp_path):
    """The actual click path: (t_dep, route_id) from the leg → trip identity."""
    _leg(con, T0 + 100, trip_id="85:11:AAA", route_id=7)
    _leg(con, T0 + 100, trip_id="85:11:BBB", route_id=9)  # same second, different pair
    out = tmp_path / "j.parquet"
    ingest.export_journeys_day(con, DAY, out)

    got = duckdb.sql(f"""
        SELECT trip_id FROM read_parquet('{out.as_posix()}')
        WHERE t_dep = {T0 + 100} AND route_id = 7""").fetchall()
    assert got == [("85:11:AAA",)]


def test_trip_id_alone_would_merge_two_service_days(con, tmp_path):
    """Why service_day is carried: (trip_id, service_day) is the journey key, trip_id isn't.

    A 00:30 departure belongs to the previous service day, so one file holds both.
    """
    _leg(con, T0 + 1800, trip_id="85:11:1", service_day=date(2018, 5, 2))  # 00:30, late service
    _leg(con, T0 + 40000, trip_id="85:11:1", service_day=DAY)  # same id, next day's run
    out = tmp_path / "j.parquet"
    ingest.export_journeys_day(con, DAY, out)

    rows = duckdb.sql(f"""
        SELECT service_day, count(*) FROM read_parquet('{out.as_posix()}')
        WHERE trip_id = '85:11:1' GROUP BY 1 ORDER BY 1""").fetchall()
    assert rows == [(date(2018, 5, 2), 1), (DAY, 1)]


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
    assert ingest.export_journeys_day(con, DAY, out) == {"rows": 0, "bytes": 0}
    assert not out.exists()


def test_route_pairs_json_maps_id_to_both_ends(con, tmp_path):
    con.execute("INSERT INTO station_pairs VALUES (8507000, 8507100, 1), (8501120, 8501008, 2)")
    out = tmp_path / "route_pairs.json"
    stats = ingest.export_route_pairs(con, out)

    import json

    assert stats["pairs"] == 2
    assert json.loads(out.read_text()) == {"1": [8507000, 8507100], "2": [8501120, 8501008]}
