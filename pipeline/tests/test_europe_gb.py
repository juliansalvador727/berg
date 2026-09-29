"""The British adapter, on hand-written Darwin Push Port messages.

Each fixture service encodes one rule from gb.py's docstring. Messages are the archive's
shape: one Pport document per line, in hourly gzipped files named by local time.
"""

import gzip
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from berg_pipeline import paths
from berg_pipeline.europe import gb

NS = (
    'xmlns="http://www.thalesgroup.com/rtti/PushPort/v18" '
    'xmlns:s5="http://www.thalesgroup.com/rtti/PushPort/Schedules/v3" '
    'xmlns:fc="http://www.thalesgroup.com/rtti/PushPort/Forecasts/v4"'
)


def pport(ts: str, body: str) -> str:
    return f'<?xml version="1.0" encoding="utf-8"?><Pport {NS} ts="{ts}" version="18.0"><uR>{body}</uR></Pport>\n'


def schedule(rid, ssd, locs, toc="VT", cat=None, status=None, passenger=None, train="1A01"):
    attrs = f'rid="{rid}" uid="X{rid[-5:]}" trainId="{train}" ssd="{ssd}" toc="{toc}"'
    if cat:
        attrs += f' trainCat="{cat}"'
    if status:
        attrs += f' status="{status}"'
    if passenger is not None:
        attrs += f' isPassengerSvc="{str(passenger).lower()}"'
    return f"<schedule {attrs}>{''.join(locs)}</schedule>"


def loc(kind, tpl, **t):
    return f'<s5:{kind} tpl="{tpl}" ' + " ".join(f'{k}="{v}"' for k, v in t.items()) + " />"


def status(rid, ssd, tpl, key: dict, arr=None, dep=None, removed=False):
    k = " ".join(f'{a}="{v}"' for a, v in key.items())
    ev = ""
    if arr:
        ev += f'<fc:arr at="{arr}" atClass="Automatic" src="TD" />'
    if dep:
        ev += f'<fc:dep at="{dep}" atClass="Automatic" src="TD" />'
    if removed:
        ev += '<fc:arr atRemoved="true" src="TD" />'
    return f'<TS rid="{rid}" ssd="{ssd}"><fc:Location tpl="{tpl}" {k}>{ev}</fc:Location></TS>'


SUMMER = date(2025, 9, 8)
AUTUMN = date(2025, 10, 25)  # clocks go back at 02:00 BST on the 26th
SPRING = date(2026, 3, 28)  # clocks go forward at 01:00 GMT on the 29th


def utc(y, mo, d, h, mi) -> int:
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp())


def write_day(day: date, lines_by_hour: dict[int, list[str]]) -> None:
    for hour in range(24):
        p = gb.hour_path(day, hour)
        p.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(p, "wt") as f:
            f.writelines(lines_by_hour.get(hour, []))


@pytest.fixture
def gb_root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setattr(paths, "DATA_ROOT", tmp_path)
    return tmp_path


def staged(day: date, lines_by_day: dict[date, dict[int, list[str]]]):
    for d in (day - timedelta(days=1), day, day + timedelta(days=1)):
        write_day(d, lines_by_day.get(d, {}))
        gb.extract_day(d)
    con = duckdb.connect()
    stats = gb.stage_days(con, [day])
    return con, stats


def rows(con, rid):
    return con.execute(
        """SELECT station_id, sched_arr, sched_dep, act_arr, act_dep, arr_measured, dep_measured
           FROM stg_stops WHERE trip_id = ? ORDER BY stop_seq""",
        [rid],
    ).fetchall()


def test_station_id_matches_sql_and_is_unique():
    codes = ["EUSTON", "MNCRPIC", "STPX", "STPXBOX", "A", "Z9", "PADTLL"]
    con = duckdb.connect()
    for c in codes:
        assert con.execute(f"SELECT {gb._station_id_sql(repr(c))}").fetchone()[0] == gb.station_id(
            c
        )
    assert len({gb.station_id(c) for c in codes}) == len(codes)
    assert gb.station_id("ZZZZZZZ") < 2**53


def test_public_calls_actuals_and_latest_schedule(gb_root):
    rid = "202509080000001"
    old = schedule(
        rid,
        "2025-09-08",
        [
            loc("OR", "EUSTON", ptd="10:00", wtd="10:00"),
            loc("DT", "MKC", pta="10:40", wta="10:40"),
        ],
    )
    new = schedule(
        rid,
        "2025-09-08",
        [
            loc("OR", "EUSTON", ptd="10:00", wtd="10:00"),
            loc("PP", "WATFDJ", wtp="10:15"),
            loc("IP", "MKC", pta="10:40", ptd="10:42", wta="10:40", wtd="10:42"),
            loc("DT", "MNCRPIC", pta="12:10", wta="12:10"),
        ],
    )
    lines = {
        2: [pport("2025-09-08T02:00:00+01:00", old)],
        9: [pport("2025-09-08T09:00:00+01:00", new)],
        10: [
            pport(
                "2025-09-08T10:01:00+01:00",
                status(rid, "2025-09-08", "EUSTON", {"wtd": "10:00", "ptd": "10:00"}, dep="10:01"),
            ),
            # A forecast is never an observation.
            pport(
                "2025-09-08T10:30:00+01:00",
                '<TS rid="%s" ssd="2025-09-08"><fc:Location tpl="MKC" wta="10:40" wtd="10:42">'
                '<fc:arr et="10:44" src="Darwin" /></fc:Location></TS>' % rid,
            ),
            pport(
                "2025-09-08T10:45:00+01:00",
                status(rid, "2025-09-08", "MKC", {"wta": "10:40", "wtd": "10:42"}, arr="10:45"),
            ),
        ],
    }
    con, stats = staged(SUMMER, {SUMMER: lines})
    got = rows(con, rid)
    assert [r[0] for r in got] == [gb.station_id(c) for c in ("EUSTON", "MKC", "MNCRPIC")]
    euston, mkc, man = got
    assert euston[2] == utc(2025, 9, 8, 9, 0) and euston[4] == utc(2025, 9, 8, 9, 1)
    assert mkc[3] == utc(2025, 9, 8, 9, 45) and mkc[5] and not mkc[6]
    assert man[3] is None and not man[5]
    assert stats["passenger_services"] == 1


def test_non_passenger_services_are_dropped(gb_root):
    locs = [loc("OR", "A", ptd="10:00", wtd="10:00"), loc("DT", "B", pta="10:10", wta="10:10")]
    lines = {
        2: [
            pport("2025-09-08T02:00:00+01:00", s)
            for s in (
                schedule("202509080000010", "2025-09-08", locs, cat="EE"),
                schedule("202509080000011", "2025-09-08", locs, status="B"),
                schedule("202509080000012", "2025-09-08", locs, passenger=False),
                schedule("202509080000013", "2025-09-08", locs, toc="LT"),
                schedule("202509080000014", "2025-09-08", locs),
            )
        ]
    }
    con, _ = staged(SUMMER, {SUMMER: lines})
    assert con.execute("SELECT DISTINCT trip_id FROM stg_stops").fetchall() == [
        ("202509080000014",)
    ]


def test_past_midnight_stays_on_its_service_day(gb_root):
    rid = "202509080000020"
    s = schedule(
        rid,
        "2025-09-08",
        [
            loc("OR", "A", ptd="23:50", wtd="23:50"),
            loc("IP", "B", pta="23:59", ptd="00:01", wta="23:59:30", wtd="00:01"),
            loc("DT", "C", pta="00:20", wta="00:20"),
        ],
    )
    nxt = SUMMER + timedelta(days=1)
    con, _ = staged(
        SUMMER,
        {
            SUMMER: {2: [pport("2025-09-08T02:00:00+01:00", s)]},
            nxt: {
                0: [
                    pport(
                        "2025-09-09T00:22:00+01:00",
                        status(rid, "2025-09-08", "C", {"wta": "00:20"}, arr="00:22"),
                    )
                ]
            },
        },
    )
    a, b, c = rows(con, rid)
    assert b[1] == utc(2025, 9, 8, 22, 59) and b[2] == utc(2025, 9, 8, 23, 1)
    assert c[1] == utc(2025, 9, 8, 23, 20) and c[3] == utc(2025, 9, 8, 23, 22)
    assert con.execute("SELECT DISTINCT service_day FROM stg_stops").fetchall() == [(SUMMER,)]


def test_clock_change_night_keeps_the_timetable_offset(gb_root):
    # Measured on 9W18 of 2025-10-25: Darwin times the whole run in BST (Harlington 01:58,
    # Bedford 02:14), while the actuals are the wall clock — 01:57 at Harlington before the
    # clocks went back at 02:00 BST, 01:10 at Bedford after.
    rid = "202510250000030"
    s = schedule(
        rid,
        "2025-10-25",
        [
            loc("OR", "BRGHTN", ptd="23:23", wtd="23:23"),
            loc("IP", "HRLG", pta="01:58", ptd="01:58", wta="01:58", wtd="01:58"),
            loc("DT", "BEDFDM", pta="02:14", wta="02:14"),
        ],
    )
    nxt = AUTUMN + timedelta(days=1)
    con, _ = staged(
        AUTUMN,
        {
            AUTUMN: {2: [pport("2025-10-25T02:00:00+01:00", s)]},
            nxt: {
                1: [
                    pport(
                        "2025-10-26T01:57:00+01:00",
                        status(
                            rid, "2025-10-25", "HRLG", {"wta": "01:58", "wtd": "01:58"}, dep="01:57"
                        ),
                    ),
                    pport(
                        "2025-10-26T01:10:00+00:00",
                        status(rid, "2025-10-25", "BEDFDM", {"wta": "02:14"}, arr="01:10"),
                    ),
                ]
            },
        },
    )
    _, a, b = rows(con, rid)
    assert a[2] == utc(2025, 10, 26, 0, 58) and a[4] == utc(2025, 10, 26, 0, 57)
    assert b[1] == utc(2025, 10, 26, 1, 14) and b[3] == utc(2025, 10, 26, 1, 10)


def test_spring_clock_change_night_is_wall_clock(gb_root):
    # Measured on 2026-03-28's Bedford 23:36 → Brighton: St Pancras 00:54 GMT, then
    # Blackfriars 02:02 BST eight minutes later, and Gatwick (02:56) reached at 03:02 BST.
    rid = "202603280000050"
    s = schedule(
        rid,
        "2026-03-28",
        [
            loc("OR", "BEDFDM", ptd="23:36", wtd="23:36"),
            loc("IP", "STPXBOX", pta="00:53", ptd="00:54", wta="00:53", wtd="00:54"),
            loc("IP", "BLFR", pta="02:02", ptd="02:02", wta="02:02", wtd="02:02"),
            loc("DT", "GTWK", pta="02:56", wta="02:56:30"),
        ],
    )
    nxt = SPRING + timedelta(days=1)
    con, _ = staged(
        SPRING,
        {
            SPRING: {2: [pport("2026-03-28T02:00:00+00:00", s)]},
            nxt: {
                3: [
                    pport(
                        "2026-03-29T03:02:00+01:00",
                        status(rid, "2026-03-28", "GTWK", {"wta": "02:56:30"}, arr="03:02"),
                    )
                ]
            },
        },
    )
    _, stp, blfr, gtwk = rows(con, rid)
    assert stp[2] == utc(2026, 3, 29, 0, 54)
    assert blfr[1] == utc(2026, 3, 29, 1, 2)
    assert gtwk[1] == utc(2026, 3, 29, 1, 56) and gtwk[3] == utc(2026, 3, 29, 2, 2)


def test_cancelled_calls_keep_no_times_and_removed_actuals_drop(gb_root):
    rid = "202509080000040"
    s = schedule(
        rid,
        "2025-09-08",
        [
            loc("OR", "A", ptd="10:00", wtd="10:00"),
            loc("IP", "B", pta="10:10", ptd="10:11", wta="10:10", wtd="10:11", can="true"),
            loc("DT", "C", pta="10:20", wta="10:20"),
        ],
    )
    con, stats = staged(
        SUMMER,
        {
            SUMMER: {
                2: [pport("2025-09-08T02:00:00+01:00", s)],
                10: [
                    pport(
                        "2025-09-08T10:21:00+01:00",
                        status(rid, "2025-09-08", "C", {"wta": "10:20"}, arr="10:21"),
                    ),
                    pport(
                        "2025-09-08T10:25:00+01:00",
                        status(rid, "2025-09-08", "C", {"wta": "10:20"}, removed=True),
                    ),
                ],
            }
        },
    )
    a, b, c = rows(con, rid)
    assert b[1:5] == (None, None, None, None)
    assert c[3] is None and not c[5]
    assert stats["part_cancelled_services"] == 1


def test_consecutive_calls_at_one_station_are_one_stop(gb_root):
    # A train that reverses is listed twice at the same TIPLOC; it must not become a leg.
    rid = "202509080000060"
    s = schedule(
        rid,
        "2025-09-08",
        [
            loc("OR", "A", ptd="10:00", wtd="10:00"),
            loc("DT", "VIRGINW", pta="10:10", wta="10:10"),
            loc("OR", "VIRGINW", ptd="10:20", wtd="10:20"),
            loc("DT", "C", pta="10:30", wta="10:30"),
        ],
    )
    con, _ = staged(SUMMER, {SUMMER: {2: [pport("2025-09-08T02:00:00+01:00", s)]}})
    got = rows(con, rid)
    assert [r[0] for r in got] == [gb.station_id(c) for c in ("A", "VIRGINW", "C")]
    assert got[1][1] == utc(2025, 9, 8, 9, 10) and got[1][2] == utc(2025, 9, 8, 9, 20)
