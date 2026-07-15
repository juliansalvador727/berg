"""GTFS snapshot listing rules.

The filename shapes here are all real. The middle one cost a silent bug: a date regex anchored
with '$' straight after the date matched 435 of 722 links and dropped every snapshot from 2021
to 2023 — without an error. Hence test_every_real_filename_shape_parses and the raise in
list_snapshots.
"""

from datetime import date, datetime

import pytest

from berg_pipeline.gtfs import Snapshot, latest_per_day, stops_member

BASE = "https://archive.opentransportdata.swiss"


def _snap(name: str, published: datetime, fp: int = 2021) -> Snapshot:
    return Snapshot(url=f"{BASE}/timetable_gtfs/x/{name}", published=published, fp_year=fp)


@pytest.mark.parametrize(
    ("name", "expect"),
    [
        ("GTFS_FP2017_2017-01-23.zip", datetime(2017, 1, 23, 0, 0)),  # hyphenated
        ("GTFS_FP2021_2021-02-03_10-01.zip", datetime(2021, 2, 3, 10, 1)),  # + publish time
        ("GTFS_FP2026_20260425.zip", datetime(2026, 4, 25, 0, 0)),  # compact
    ],
)
def test_every_real_filename_shape_parses(name, expect):
    from berg_pipeline.gtfs import _NAME_RE

    m = _NAME_RE.search(name)
    assert m is not None, f"{name} did not parse"
    got = datetime(int(m["y"]), int(m["m"]), int(m["d"]), int(m["hh"] or 0), int(m["mm"] or 0))
    assert got == expect


def test_latest_per_day_keeps_the_last_publish():
    # 2021-02-03 published twice; a day-grained validity range built from both would be
    # zero-length, so the later publish must win.
    a = _snap("GTFS_FP2021_2021-02-03_10-01.zip", datetime(2021, 2, 3, 10, 1))
    b = _snap("GTFS_FP2021_2021-02-03_15-06.zip", datetime(2021, 2, 3, 15, 6))
    c = _snap("GTFS_FP2021_2021-02-10_09-40.zip", datetime(2021, 2, 10, 9, 40))

    got = latest_per_day([a, b, c])

    assert [s.day for s in got] == [date(2021, 2, 3), date(2021, 2, 10)]
    assert got[0].published == datetime(2021, 2, 3, 15, 6)


def test_latest_per_day_is_order_independent():
    a = _snap("GTFS_FP2021_2021-02-03_10-01.zip", datetime(2021, 2, 3, 10, 1))
    b = _snap("GTFS_FP2021_2021-02-03_15-06.zip", datetime(2021, 2, 3, 15, 6))
    assert latest_per_day([b, a])[0].published == datetime(2021, 2, 3, 15, 6)


def test_stops_member_finds_nested_and_flat():
    assert stops_member(["stops.txt", "trips.txt"]) == "stops.txt"
    assert stops_member(["gtfs/stops.txt", "gtfs/routes.txt"]) == "gtfs/stops.txt"


def test_stops_member_raises_rather_than_returning_none():
    with pytest.raises(LookupError):
        stops_member(["routes.txt", "trips.txt"])
