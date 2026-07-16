"""The dim_station SCD2 rules, on synthetic GTFS snapshots small enough to check by hand.

The archive's stops feed is noisy in a specific way: a station drops out of a snapshot or two
and comes back byte-identical. Whether that counts as a change is the whole design of this
table, and getting it wrong is quiet — it does not fail, it just stops matching legs.
"""

import gzip
from pathlib import Path

import duckdb
import pytest

from berg_pipeline.dim_station import build

HEADER = "stop_id,stop_name,stop_lat,stop_lon,location_type"


def snapshot(cache: Path, day: str, rows: list[str]) -> None:
    """One stops-YYYY-MM-DD.txt.gz, as the GTFS mirror publishes it."""
    path = cache / f"stops-{day}.txt.gz"
    path.write_bytes(gzip.compress(("\n".join([HEADER, *rows]) + "\n").encode()))


def stop(bpuic: int, name: str = "Assens", lat: float = 46.61315, lon: float = 6.62053) -> str:
    return f"{bpuic},{name},{lat},{lon},1"


@pytest.fixture
def cache(tmp_path: Path) -> Path:
    d = tmp_path / "gtfs"
    d.mkdir()
    return d


def ranges(out: Path, bpuic: int) -> list[tuple]:
    con = duckdb.connect()
    return con.execute(
        f"""SELECT valid_from, valid_to FROM read_parquet('{out.as_posix()}')
            WHERE bpuic = {bpuic} ORDER BY valid_from"""
    ).fetchall()


def test_absence_with_unchanged_attributes_does_not_split(cache, tmp_path):
    """The 2.0M-leg bug: Assens vanishes for a snapshot and returns identical.

    It never moved and trains kept calling there throughout. Splitting here puts a hole in the
    validity range, and every leg landing in that hole is quarantined as 'unmatched_station' —
    which reads exactly like the foreign-station noise the pipeline legitimately expects, so
    nothing ever complains.
    """
    snapshot(cache, "2024-01-01", [stop(8501172)])
    snapshot(cache, "2024-01-08", [stop(9999999, name="Elsewhere")])  # Assens absent
    snapshot(cache, "2024-01-15", [stop(8501172)])
    out = tmp_path / "dim.parquet"
    build(cache, out)

    got = ranges(out, 8501172)
    assert len(got) == 1, f"absence split an unchanged station into {len(got)} ranges"
    assert str(got[0][0]) == "2024-01-01" and str(got[0][1]) == "9999-12-31 00:00:00"


def test_moving_and_moving_back_still_splits(cache, tmp_path):
    """The bound on the rule above: attributes, not presence, are what segment.

    A->B->A must stay three ranges. Merging by attribute value instead would fuse the two A
    segments into one span straddling B, and overlapping ranges make the fact join emit
    duplicate legs — the failure build() raises on.
    """
    snapshot(cache, "2024-01-01", [stop(8501172, lat=46.61315)])
    snapshot(cache, "2024-01-08", [stop(8501172, lat=46.70000)])
    snapshot(cache, "2024-01-15", [stop(8501172, lat=46.61315)])
    out = tmp_path / "dim.parquet"
    build(cache, out)  # raises if any range overlaps

    got = ranges(out, 8501172)
    assert len(got) == 3, f"a real move must still segment, got {len(got)} ranges"


def test_absence_at_the_end_closes_the_range(cache, tmp_path):
    """A station that leaves and never returns still gets closed, not carried to 9999."""
    snapshot(cache, "2024-01-01", [stop(8501172)])
    snapshot(cache, "2024-01-08", [stop(9999999, name="Elsewhere")])
    out = tmp_path / "dim.parquet"
    build(cache, out)

    got = ranges(out, 8501172)
    assert len(got) == 1
    assert str(got[0][1]) == "2024-01-07 00:00:00", (
        "valid_to should be the day before the snapshot that dropped it"
    )
