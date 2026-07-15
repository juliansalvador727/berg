"""Archive URL and member rules.

Every case here is a real shape observed in the archive census — see docs/data-notes.md.
"""

import zipfile
from datetime import date

import pytest

from berg_pipeline.archive import (
    day_members,
    is_data_member,
    member_date,
    url_for_month,
)


def test_url_naming_eras():
    assert (
        url_for_month(2018, 1) == "https://archive.opentransportdata.swiss/istdaten/2018/18_01.zip"
    )
    assert (
        url_for_month(2021, 5) == "https://archive.opentransportdata.swiss/istdaten/2021/21_05.zip"
    )
    # 2021-06 is where the long-form name starts.
    assert (
        url_for_month(2021, 6)
        == "https://archive.opentransportdata.swiss/istdaten/2021/ist-daten-2021-06.zip"
    )
    assert (
        url_for_month(2025, 7, v2=True)
        == "https://archive.opentransportdata.swiss/istdaten/2025/ist-daten-v2-2025-07.zip"
    )


def test_url_rejects_unusable_ranges():
    # 2016+2017 ship as one "unvollstaendig" ZIP; treating them as normal months would
    # silently produce a fifth of a year of data.
    with pytest.raises(ValueError):
        url_for_month(2017, 12)
    with pytest.raises(ValueError):
        url_for_month(2025, 6, v2=True)


@pytest.mark.parametrize(
    "name",
    [
        "jan18/2018-01-01istdaten.csv",
        "19_7/2019-07-01istdaten.csv",
        "20_04/2020-04-01_istdaten.csv",
        "2022-01-01_istdaten.csv",
        "ist-daten-2023-04/2023-04-01_istdaten.csv",
        "2026-06-03_IstDaten.csv",
    ],
)
def test_real_member_paths_are_data(name):
    assert is_data_member(name)
    assert member_date(name) is not None


@pytest.mark.parametrize(
    "name",
    [
        "__MACOSX/._2023-03-01_istdaten.csv",  # AppleDouble fork: a .csv name, ~300 bytes
        "__MACOSX/._2024-11-19_IstDaten.csv",
        "readme.txt",
    ],
)
def test_junk_members_rejected(name):
    assert not is_data_member(name)


def test_member_date_reads_the_basename_not_the_folder():
    # The folder says 2023-04 and so does the file; but only the basename is authoritative,
    # because folder names have used every era's convention.
    assert member_date("ist-daten-2023-04/2023-04-17_istdaten.csv") == date(2023, 4, 17)
    assert member_date("jan18/2018-01-31istdaten.csv") == date(2018, 1, 31)


def _zip(tmp_path, members: dict[str, int]):
    p = tmp_path / "m.zip"
    with zipfile.ZipFile(p, "w") as zf:
        for name, size in members.items():
            zf.writestr(name, b"x" * size)
    return zipfile.ZipFile(p)


def test_day_members_filters_junk_and_keeps_real(tmp_path):
    # 2023-03's actual shape: every real day shadowed by a resource fork.
    with _zip(
        tmp_path,
        {
            "2023-03-01_istdaten.csv": 5000,
            "__MACOSX/._2023-03-01_istdaten.csv": 300,
            "2023-03-02_istdaten.csv": 5000,
            "__MACOSX/._2023-03-02_istdaten.csv": 300,
        },
    ) as zf:
        got = day_members(zf)
    assert set(got) == {date(2023, 3, 1), date(2023, 3, 2)}
    assert all(i.file_size == 5000 for i in got.values())


def test_day_members_prefers_the_largest_duplicate(tmp_path):
    with _zip(
        tmp_path,
        {"a/2024-05-01_istdaten.csv": 100, "b/2024-05-01_istdaten.csv": 9000},
    ) as zf:
        got = day_members(zf)
    assert got[date(2024, 5, 1)].file_size == 9000
