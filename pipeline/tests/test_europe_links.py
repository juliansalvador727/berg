"""The cross-border layer on two hand-built publish mirrors around Basel.

Switzerland ("ch") carries the German border belt, so it has Weil am Rhein too, under its own
UIC-prefixed id. Germany ("de") ends ICE 275 at Freiburg; Switzerland starts it at Basel Bad
Bf. The files have the published shapes: stations.json, route_pairs.json, legs and journey
parquet per UTC day, and a manifest listing the days.
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pytest

from berg_pipeline.europe import links

DAY = date(2026, 3, 10)


def t(hh: int, mm: int) -> int:
    return int(datetime(2026, 3, 10, hh, mm, tzinfo=timezone.utc).timestamp())


CH_STATIONS = [
    (8500090, "Basel Bad Bf", 7.6075, 47.5673),
    (8500010, "Basel SBB", 7.5895, 47.5474),
    (8014442, "Weil am Rhein", 7.6205, 47.5906),  # a German station in the Swiss archive
]
DE_STATIONS = [
    (8000107, "Freiburg (Breisgau) Hbf", 7.8412, 47.9977),
    (8000290, "Offenburg", 7.9464, 48.4765),
    (8000108, "Müllheim im Markgräflerland", 7.6295, 47.8085),
    (8006000, "Weil am Rhein", 7.6207, 47.5905),
]


def write_source(root: Path, stations, pairs: dict[int, tuple[int, int]], legs, journeys):
    static = root / "static"
    static.mkdir(parents=True)
    (static / "stations.json").write_text(json.dumps(
        [{"id": i, "name": n, "code": None, "lon": lo, "lat": la} for i, n, lo, la in stations]))
    (static / "route_pairs.json").write_text(json.dumps({str(k): list(v) for k, v in pairs.items()}))
    (root / "manifest.json").write_text(json.dumps({"days": {DAY.isoformat(): {}}}))
    con = duckdb.connect()
    for kind, rows, ddl in (
        ("legs", legs, "route_id UINTEGER, journey_id USMALLINT, t_dep UINTEGER, dur USMALLINT, "
                       "type UTINYINT, delay SMALLINT, flags UTINYINT"),
        ("journeys", journeys, "journey_id USMALLINT, trip_id VARCHAR, service_day DATE, "
                               "line VARCHAR"),
    ):
        out = root / kind / f"{DAY:%Y/%m/%d}.parquet"
        out.parent.mkdir(parents=True)
        con.execute(f"CREATE OR REPLACE TABLE x ({ddl})")
        con.executemany(f"INSERT INTO x VALUES ({', '.join('?' * len(rows[0]))})", rows)
        con.execute(f"COPY x TO '{out.as_posix()}' (FORMAT PARQUET)")


@pytest.fixture
def world(tmp_path: Path, monkeypatch) -> dict[str, links.Source]:
    ch = links.Source("ch", "CH", "observed", tmp_path / "ch", tmp_path / "ch.parquet",
                      (5.9, 45.8, 10.5, 47.9))
    de = links.Source("de", "DE", "final_prediction", tmp_path / "de", tmp_path / "de.parquet",
                      (5.8, 47.2, 15.1, 55.1))
    for src, stations in ((ch, CH_STATIONS), (de, DE_STATIONS)):
        duckdb.sql(f"COPY (SELECT * FROM (VALUES {', '.join(f'({s[0]})' for s in stations)}) "
                   f"v(bpuic)) TO '{src.dim.as_posix()}' (FORMAT PARQUET)")
    write_source(
        ch.publish_root, CH_STATIONS,
        {1: (8500090, 8500010), 2: (8014442, 8500090), 3: (8500010, 8500090)},
        legs=[
            # ICE 275 from Basel Bad Bf, 35 minutes after Germany's last stop at Freiburg.
            (1, 0, t(10, 35), 300, 7, 0, 0),
            # RB 17001 carried on from Weil am Rhein, where Germany's journey ends.
            (2, 1, t(10, 50), 300, 3, 60, 0),
            # S 999 at Basel SBB: its number matches a German train that ends at Freiburg.
            (3, 2, t(11, 0), 240, 5, 0, 0),
        ],
        journeys=[(0, "85:11:275:001", DAY, None), (1, "80:800693:17001:000", DAY, "RB"),
                  (2, "ch:1:sjyid:100001:999-001", DAY, "S1")],
    )
    write_source(
        de.publish_root, DE_STATIONS,
        {1: (8000290, 8000107), 2: (8000108, 8006000), 3: (8000290, 8000107)},
        legs=[
            (1, 0, t(9, 30), 1800, 1, 60, 0),  # ICE 275 Offenburg → Freiburg, arr 10:00
            (2, 1, t(10, 30), 1080, 2, 0, 0),  # RB 17001 Müllheim → Weil, arr 10:48
            (3, 2, t(9, 0), 1800, 4, 0, 0),  # S 999 Offenburg → Freiburg, arr 09:30
        ],
        journeys=[(0, "ICE 275", DAY, None), (1, "RB 17001", DAY, "RB"), (2, "S 999", DAY, "S1")],
    )
    return {"ch": ch, "de": de}


def served(world):
    return {k: links.served_stations(s) for k, s in world.items()}


def test_crosswalk_joins_one_station_across_datasets(world):
    cw = links.build_crosswalk(served(world))
    names = [sorted((m["dataset"], m["name"]) for m in g["members"]) for g in cw["groups"]]
    assert names == [[("ch", "Weil am Rhein"), ("de", "Weil am Rhein")]]
    group = cw["groups"][0]
    assert group["confidence"] == "high" and group["max_distance_m"] < 50
    # The Swiss archive's German station is German by its UIC prefix.
    assert {m["country"] for m in group["members"]} == {"DE"}


def test_links_bridge_the_gap_and_hand_over_at_a_shared_station(world):
    stations = served(world)
    canon = links.canonical_map(links.build_crosswalk(stations))
    con = duckdb.connect()
    found = {(lk["train"], lk["kind"]): lk for lk in links.one_per_end(
        links.link_day(con, world["ch"], world["de"], DAY, stations, canon))}

    ice = found[("275", "bridge")]
    assert ice["from"] == ["de", 0] and ice["to"] == ["ch", 0]
    assert (ice["bridge"]["from_station"], ice["bridge"]["to_station"]) == (8000107, 8500090)
    assert (ice["bridge"]["t_dep"], ice["bridge"]["dur"]) == (t(10, 0), 35 * 60)
    assert ice["at"] == t(10, 35)

    rb = found[("17001", "handover")]
    assert rb["from"] == ["de", 1] and rb["to"] == ["ch", 1] and "bridge" not in rb
    # S 999 matches by number and could make the trip, so it is a candidate here; the
    # build-wide pair judgement is what rejects it.
    assert ("999", "bridge") in found


def test_a_bridge_never_spans_what_the_other_dataset_sees(world):
    """Switzerland serves Weil am Rhein. A German train ending there that really ran on would be
    in the Swiss journey, so it is never bridged onward to Basel SBB."""
    stations = served(world)
    canon = links.canonical_map(links.build_crosswalk(stations))
    first = (1, "RB 17001", [{"station": 8006000, "t": t(10, 48), "side": "arr",
                              "type": 2, "delay": 0}])
    second = (2, "S 17001", [{"station": 8500010, "t": t(11, 5), "side": "dep",
                              "type": 5, "delay": 0}])
    seen_by = {}
    for (ds, _), g in canon.items():
        seen_by.setdefault(g, set()).add(ds)
    assert links._judge(first, second, world["de"], world["ch"], stations, canon, seen_by) is None


def bridge_link(frm, to, number, dur, day):
    return {"kind": "bridge", "from": [frm[0], 0], "to": [to[0], 0], "train": number,
            "at": 0, "bridge": {"from_station": frm[1], "to_station": to[1], "dur": dur}}


def test_bridge_pairs_are_judged_over_the_whole_build():
    days = [date(2026, 3, d) for d in range(1, 11)]
    real = ("de", 8000107), ("ch", 8500090)
    fake = ("be", 1), ("nl", 2)
    per_day = {
        d: [bridge_link(*real, str(100 + i), 2100 + 60 * i, d) for i in range(6)]
        + [bridge_link(real[1], real[0], str(200 + i), 2000, d) for i in range(6)]
        # One number, every day, one way: timetables repeat, so do coincidences.
        + [bridge_link(*fake, "3626", 4200, d)]
        for d in days
    }
    edges = {k: 1_000.0 for k in [("de", 8000107, "ch"), ("ch", 8500090, "de"),
                                   ("be", 1, "nl"), ("nl", 2, "be")]}
    verdicts = links.judge_bridge_pairs(per_day, edges)
    assert verdicts[("de", 8000107, "ch", 8500090)]["accepted"]
    assert verdicts[("ch", 8500090, "de", 8000107)]["both_ways"]
    fake_verdict = verdicts[("be", 1, "nl", 2)]
    assert not fake_verdict["accepted"] and fake_verdict["numbers"] == 1

    far = dict(edges) | {("de", 8000107, "ch"): 80_000.0}
    assert not links.judge_bridge_pairs(per_day, far)[("de", 8000107, "ch", 8500090)]["accepted"]


def test_one_continuation_per_journey_across_all_datasets():
    """Aachen–Heerlen–Liège: the German journey continues into the Dutch one, not also straight
    into the Belgian one across the Dutch section."""
    to_nl = {"kind": "bridge", "from": ["de", 5], "to": ["nl", 7], "at": 2,
             "bridge": {"dur": 300}}
    to_be = {"kind": "bridge", "from": ["de", 5], "to": ["be", 9], "at": 3,
             "bridge": {"dur": 2700}}
    assert links.one_per_end([to_be, to_nl]) == [to_nl]


def test_build_writes_the_layer(world, tmp_path, monkeypatch):
    monkeypatch.setattr(links, "sources", lambda: world)
    for name, value in (("BRIDGE_MIN_DAYS", 1), ("BRIDGE_MIN_NUMBERS", 1),
                        ("BRIDGE_MIN_NUMBERS_ONE_WAY", 1), ("BRIDGE_MIN_PER_DAY", 1)):
        monkeypatch.setattr(links, name, value)
    manifest = links.build(tmp_path / "links", log=lambda *a: None)
    publish = tmp_path / "links" / "publish"
    day = json.loads((publish / "days" / "2026" / "03" / "10.json").read_text())
    assert {link["train"] for link in day["links"]} == {"275", "17001", "999"}
    assert manifest["bridge_routes"] == 2  # Freiburg → Basel Bad Bf, Freiburg → Basel SBB
    assert manifest["pairs"] == [["ch", "de"]]
    assert json.loads((publish / "crosswalk.json").read_text())["groups"]
    pairs = duckdb.connect(str(tmp_path / "links" / "geometry" / "bridges.duckdb"),
                           read_only=True).execute("SELECT * FROM station_pairs").fetchall()
    assert sorted(pairs) == [(1, 5_008_000_107, 1_008_500_010), (2, 5_008_000_107, 1_008_500_090)]
