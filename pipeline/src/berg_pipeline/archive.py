"""URL construction and member selection for the Ist-Daten archive.

There is no single URL pattern — the archive has been renamed three times and never
backfilled, so era handling is a fact of life, not a transitional hack. Member paths inside
the ZIPs drift just as much. See docs/data-notes.md.
"""

import re
import zipfile
from datetime import date

BASE = "https://archive.opentransportdata.swiss/istdaten"

# Member paths have taken every shape: 'jan18/2018-01-01istdaten.csv', '19_7/...',
# '20_04/2020-04-01_istdaten.csv', bare '2022-01-01_istdaten.csv',
# 'ist-daten-2023-04/2023-04-01_istdaten.csv'. The one stable thing is an ISO date in the
# basename — match that, never construct a path.
MEMBER_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# Some months were zipped on a Mac and carry AppleDouble resource forks: a parallel
# '__MACOSX/._2023-03-01_istdaten.csv' for every real member. They are ~300-byte binaries
# with a .csv name, so a naive glob ingests 30 of them as service days.
JUNK_PREFIXES = ("__MACOSX/",)

# A real Swiss service day is 130-400 MB of CSV. Far below that is a stub or truncated
# publish, not a quiet day — the archive genuinely has holes (2019-07-01..16, 2022-11-09).
STUB_MAX_BYTES = 20_000_000


def is_data_member(name: str) -> bool:
    """True if a ZIP member is a real day CSV rather than packaging junk."""
    base = name.rsplit("/", 1)[-1]
    return (
        name.lower().endswith(".csv")
        and not base.startswith("._")
        and not name.startswith(JUNK_PREFIXES)
    )


def member_date(name: str) -> date | None:
    m = MEMBER_DATE_RE.search(name.rsplit("/", 1)[-1])
    return date(int(m[1]), int(m[2]), int(m[3])) if m else None


def day_members(zf: zipfile.ZipFile) -> dict[date, zipfile.ZipInfo]:
    """Map service date → the real day member, junk filtered and duplicates resolved.

    Where a date appears more than once, the largest member wins: duplicates in this archive
    are stubs or resource forks shadowing the real file.
    """
    out: dict[date, zipfile.ZipInfo] = {}
    for info in zf.infolist():
        if not is_data_member(info.filename):
            continue
        d = member_date(info.filename)
        if d is None:
            continue
        if d not in out or info.file_size > out[d].file_size:
            out[d] = info
    return out


def url_for_month(year: int, month: int, v2: bool = False) -> str:
    """Archive URL for a service month.

    2016+2017 are deliberately unsupported: they ship as one "unvollstaendig" ZIP holding
    166 MB for 24 months. Usable coverage starts 2018-01.
    """
    if (year, month) < (2018, 1):
        raise ValueError(f"{year}-{month:02d} predates usable coverage (2018-01)")
    if v2:
        if (year, month) < (2025, 7):
            raise ValueError(f"v2 does not exist before 2025-07 (asked for {year}-{month:02d})")
        return f"{BASE}/{year}/ist-daten-v2-{year}-{month:02d}.zip"
    if (year, month) <= (2021, 5):
        return f"{BASE}/{year}/{year % 100:02d}_{month:02d}.zip"
    return f"{BASE}/{year}/ist-daten-{year}-{month:02d}.zip"
