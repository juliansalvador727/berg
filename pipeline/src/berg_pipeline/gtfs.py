"""The archived GTFS timetable snapshots — the source for dim_station.

DIDOK/ATLAS are dead ends (CKAN 403s, the ATLAS host does not resolve), but the same archive
publishes GTFS roughly weekly back to 2016. Each snapshot's stops.txt is a few MB inside a
~130 MB ZIP, so the range-request reader gets one cheaply.

Snapshots are published under two directory eras and two filename date formats, and the
listing is the only authority on what exists — parse it, don't construct URLs.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime

import httpx

BASE = "https://archive.opentransportdata.swiss"
INDEX = f"{BASE}/timetable_gtfs.php"

_LINK_RE = re.compile(r'href="(timetable_gtfs/[^"]+\.zip)"')

# Three filename shapes, all live:
#   GTFS_FP2017_2017-01-23.zip          (435×)  hyphenated date
#   GTFS_FP2021_2021-02-03_10-01.zip    (202×)  + publish TIME; a day can publish twice
#   GTFS_FP2026_20260425.zip             (85×)  compact date
# Anchoring '$' straight after the date silently drops the middle shape — which is every
# snapshot from 2021 to 2023. Ask how this file knows.
_NAME_RE = re.compile(
    r"GTFS_FP(?P<fp>\d{4})_(?P<y>\d{4})-?(?P<m>\d{2})-?(?P<d>\d{2})"
    r"(?:_(?P<hh>\d{2})-(?P<mm>\d{2}))?\.zip$"
)


@dataclass(frozen=True)
class Snapshot:
    url: str
    published: datetime
    fp_year: int

    @property
    def day(self) -> date:
        return self.published.date()

    def __str__(self) -> str:
        return f"{self.published:%Y-%m-%d %H:%M} FP{self.fp_year}"


def list_snapshots(timeout: float = 60.0) -> list[Snapshot]:
    """Every GTFS snapshot in the archive, oldest publication first.

    Raises if a listed .zip doesn't parse: a snapshot silently skipped is a station dimension
    that is quietly wrong for a period, which is far worse than a loud failure.
    """
    r = httpx.get(INDEX, timeout=timeout, follow_redirects=True)
    r.raise_for_status()

    out, bad = [], []
    for href in _LINK_RE.findall(r.text):
        m = _NAME_RE.search(href.rsplit("/", 1)[-1])
        if not m:
            bad.append(href)
            continue
        out.append(
            Snapshot(
                url=f"{BASE}/{href}",
                published=datetime(
                    int(m["y"]),
                    int(m["m"]),
                    int(m["d"]),
                    int(m["hh"] or 0),
                    int(m["mm"] or 0),
                ),
                fp_year=int(m["fp"]),
            )
        )
    if bad:
        raise ValueError(f"{len(bad)} GTFS snapshots did not parse, e.g. {bad[:3]}")
    return sorted(out, key=lambda s: (s.published, s.fp_year))


def latest_per_day(snaps: list[Snapshot]) -> list[Snapshot]:
    """One snapshot per publication day — the last published wins.

    Validity ranges are day-grained, so a day that published twice (2021-02-03 at 10:01 and
    15:06) would otherwise produce a zero-length range.
    """
    by_day: dict[date, Snapshot] = {}
    for s in snaps:
        if s.day not in by_day or s.published >= by_day[s.day].published:
            by_day[s.day] = s
    return [by_day[d] for d in sorted(by_day)]


def stops_member(names: list[str]) -> str:
    """The stops.txt member, wherever it sits in the ZIP."""
    hits = [n for n in names if n.rsplit("/", 1)[-1].lower() == "stops.txt"]
    if not hits:
        raise LookupError(f"no stops.txt among {len(names)} members")
    return hits[0]
