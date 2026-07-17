"""The per-day leg floor — the guard that 2023-09 got past.

That month published 28 days of 24 legs each and recorded every one in manifest.json as a
success. These tests pin the two halves of the fix: a collapsed day must fail its month, and
a collapsed month must never be skipped as "already staged".
"""

from datetime import date

import duckdb
import pytest

from berg_pipeline import archive, ingest
from berg_pipeline.constants import MIN_LEGS_PER_DAY


def _con_with_days(legs_by_day: dict[str, int]):
    """A fct_legs holding exactly `n` legs departing on each given UTC day.

    i % 86400 keeps every leg inside its own day: a day can hold more legs than it has
    seconds, and one-per-second would spill a real day's traffic into the next date.
    """
    con = duckdb.connect()
    ingest.create_tables(con)
    for day, n in legs_by_day.items():
        epoch = int((date.fromisoformat(day) - date(1970, 1, 1)).total_seconds())
        con.execute(
            """INSERT INTO fct_legs
               (service_day, trip_id, route_id, from_bpuic, to_bpuic, t_dep, dur,
                type_id, delay, flags, line)
               SELECT DATE '{d}', 'trip', 1, 1, 2, {e} + (i % 86400), 60, 1, 0, 0, 'S1'
               FROM generate_series(0, {n} - 1) s(i)""".format(d=day, e=epoch, n=n)
        )
    return con


@pytest.fixture
def censused(monkeypatch):
    """A census where 2023-09 has 30 usable days and no holes."""
    monkeypatch.setattr(
        archive, "census", lambda: {"2023-09": {"usable_days": 30, "absent": [], "stubs": []}}
    )


def test_collapsed_days_are_reported(censused):
    """The real 2023-09 shape: day 1 ingested, then the month collapsed to 24 legs a day."""
    days = {f"2023-09-{d:02d}": 24 for d in range(1, 31)}
    days["2023-09-01"] = 140_000
    con = _con_with_days(days)

    bad = ingest.validate_month_days(con, "2023-09")

    assert bad == [(f"2023-09-{d:02d}", 24) for d in range(2, 31)]


def test_a_day_at_the_floor_passes(censused):
    """The floor is a smoke alarm, not a band check — exactly at it is fine."""
    con = _con_with_days(
        dict.fromkeys([f"2023-09-{d:02d}" for d in range(1, 31)], MIN_LEGS_PER_DAY)
    )

    assert ingest.validate_month_days(con, "2023-09") == []


def test_archive_holes_are_not_failures(monkeypatch):
    """2019-07-01..16 are genuinely absent; emptiness there is correct, not a broken ingest."""
    monkeypatch.setattr(
        archive,
        "census",
        lambda: {
            "2019-07": {
                "usable_days": 15,
                "absent": [f"2019-07-{d:02d}" for d in range(1, 17)],
                "stubs": [],
            }
        },
    )
    con = _con_with_days({f"2019-07-{d:02d}": 130_000 for d in range(17, 32)} | {"2019-07-01": 0})

    assert ingest.validate_month_days(con, "2019-07") == []


def test_uncensused_month_is_not_checked(monkeypatch):
    """No ground truth, no verdict — the current unpublished month must not fail here."""
    monkeypatch.setattr(archive, "census", lambda: {})
    con = _con_with_days({"2026-07-01": 24})

    assert ingest.validate_month_days(con, "2026-07") == []
