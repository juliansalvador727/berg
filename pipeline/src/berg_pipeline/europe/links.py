"""Cross-border layer: a station crosswalk, journey links, and bridge legs between datasets.

Every dataset stops at its own edge, and not all edges are the same:

  - Switzerland keeps every leg inside its box, which reaches 47.9°N, so it also carries the
    German lines along the border (Lörrach, Waldshut, Singen, Konstanz). Germany has the same
    trains, so the same movement exists twice.
  - Germany's source never lists a foreign stop, Belgium's ends at its last Belgian point, and
    the Netherlands clips every leg with a foreign end. So an ICE from Frankfurt ends at
    Freiburg in one dataset and starts at Basel Bad Bf in the other, and the 60 km between
    them is in neither.

This module derives the pieces europe.md's identity model asks for. None of them touch a
dataset's own files:

  crosswalk.json         groups of (dataset, station) that are one physical station, with the
                         match method, distance and confidence for each group. A low-confidence
                         pair stays separate. The client uses it to recognise the same leg in
                         two datasets and draw it once (the priority rule is in DEDUP_RULE).
  days/YYYY/MM/DD.json   journey links for one UTC day: two journeys, one per dataset, that are
                         one train. Where the two do not meet, a bridge leg spans the gap from
                         the first journey's last station to the second's first. Bridge times
                         are interpolated between the two sources and labelled as such.
  static/routes.bin      track geometry for every bridge station pair, routed on the merged
                         rail networks of both countries by the ordinary geometry job.
  manifest.json          the days, the rules and the files above.

A link needs the same public train number, and either a shared station at about the same
time or a gap a train could plausibly cover across a border. Numbers alone are never enough:
S-Bahn networks reuse them within a single country.
"""

import json
import math
import shutil
import time
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from berg_pipeline import paths
from berg_pipeline.europe.config import DATASETS

LINKS_SCHEMA_VERSION = 1
LINKS_ROOT = paths.DATA_ROOT / "links"  # publish/ mirrors the bucket's links/, geometry/ is work
LINKS_KEY_PREFIX = "links/"

# Crosswalk: the same physical station in two datasets. Names are compared after
# normalization (below). A shared point within POINT_M is one station whatever it is called.
NAME_MATCH_M = 400  # high confidence: similar name, this close
SAME_NAME_M = 1_000  # medium: identical normalized name, a station complex this wide
POINT_M = 60  # medium: the same point, names disagreeing (language, abbreviation)

# Links.
SHARED_STATION_S = 10 * 60  # both journeys at one station within this of each other
HANDOVER_MAX_S = 30 * 60  # one journey ends where the other starts, within this
BRIDGE_MAX_M = 100_000  # a bridge spans at most this far
BRIDGE_MIN_S = 60
BRIDGE_MAX_S = 90 * 60
BRIDGE_MAX_SPEED_MS = 70.0  # 252 km/h over a straight line; a real train covers more track
BRIDGE_MIN_SPEED_MS = 4.0  # slower than this over a straight line is a train that waited
# A bridge candidate is judged per station pair over the whole build, because timetables
# repeat: two unrelated trains that share a number (Leuven → Weert, IC 3626 Gent →
# Roosendaal) do so every weekday. Measured over 2025-11/12, a real crossing differs on every
# count below, and a coincidence failed at least one:
BRIDGE_MIN_DAYS = 3
BRIDGE_MIN_PER_DAY = 2.0  # ... or it runs both ways
BRIDGE_MIN_NUMBERS = 5  # many trains use it, not one number repeating daily (IC 3626)
# One direction only needs more: DE S 308xx and NL ST 308xx meet Mönchengladbach →
# Winterswijk three times a day on five numbers, and never the other way.
BRIDGE_MIN_NUMBERS_ONE_WAY = 8
BRIDGE_EDGE_M = 60_000  # each end this close to a station the other dataset serves
BRIDGE_CONSISTENT_SHARE = 0.6  # most crossings take about the pair's median time
BRIDGE_DUR_TOLERANCE = 0.3  # "about": within 30% (at least 5 min) of the median

# Europe.md: "apply a documented source-priority rule to the rendered duplicate".
EVIDENCE_RANK = {"observed": 0, "final_prediction": 1, "delay_only": 2, "scheduled": 3}
DEDUP_RULE = (
    "A leg is drawn once. When two datasets hold a leg between the same crosswalked stations "
    "departing within 180 s, the dataset with stronger time evidence (observed, then "
    "final_prediction, then delay_only, then scheduled; ties to the catalog's first dataset) "
    "is drawn and the other is hidden. The hidden leg stays in its dataset, and the train's own "
    "journey keeps it when spectated."
)
DEDUP_WINDOW_S = 180

# UIC country prefixes, for the Swiss archive's foreign stations (its ids are UIC codes).
UIC_COUNTRY = {85: "CH", 80: "DE", 81: "AT", 83: "IT", 87: "FR", 84: "NL", 88: "BE", 82: "LU"}
# The Dutch station list's country codes.
NL_COUNTRY = {"D": "DE", "B": "BE", "F": "FR", "A": "AT", "I": "IT", "S": "SE", "CH": "CH",
              "NL": "NL", "GB": "GB", "DK": "DK", "PL": "PL", "CZ": "CZ", "L": "LU"}
# Geometry job station ids must be unique across datasets.
GEOMETRY_ID_PREFIX = {"ch": 1, "fi": 2, "nl": 3, "be": 4, "de": 5}

FLAG_ROUTE_FRACTION = 1 << 2


@dataclass(frozen=True)
class Source:
    """One dataset as the linker reads it: its local publish mirror."""

    dataset_id: str
    country: str
    time_semantics: str
    publish_root: Path
    dim: Path
    bbox: tuple[float, float, float, float]

    def legs(self, day: date) -> Path:
        return self.publish_root / "legs" / f"{day:%Y/%m/%d}.parquet"

    def journeys(self, day: date) -> Path:
        return self.publish_root / "journeys" / f"{day:%Y/%m/%d}.parquet"

    def days(self) -> set[date]:
        return {date.fromisoformat(d) for d in
                json.loads((self.publish_root / "manifest.json").read_text())["days"]}


def sources() -> dict[str, Source]:
    from berg_pipeline.constants import CH_BBOX

    out = {"ch": Source("ch", "CH", "observed", paths.PUBLISH_ROOT,
                        paths.DIM_STATION_PARQUET, CH_BBOX)}
    for cfg in DATASETS.values():
        out[cfg.dataset_id] = Source(cfg.dataset_id, cfg.country, cfg.time_semantics,
                                     cfg.publish_root, cfg.dim_station_parquet, cfg.bbox)
    return out


def neighbours(srcs: dict[str, Source]) -> list[tuple[str, str]]:
    """Dataset pairs whose boxes touch: the only ones that can share a train or a station."""
    ids = sorted(srcs)
    out = []
    for i, a in enumerate(ids):
        for b in ids[i + 1:]:
            wa, sa, ea, na = srcs[a].bbox
            wb, sb, eb, nb = srcs[b].bbox
            if wa <= eb and wb <= ea and sa <= nb and sb <= na:
                out.append((a, b))
    return out


# --- stations -------------------------------------------------------------------------------

_NAME_NOISE = {"bf", "bahnhof", "hbf", "station", "gare", "stazione", "centraal", "sncf", "db",
               "sbb", "cff", "ffs", "bhf", "de", "d", "la", "le", "st", "sankt", "saint"}


def normalize_name(name: str) -> frozenset[str]:
    text = unicodedata.normalize("NFKD", name.lower())
    text = "".join(c if c.isalnum() else " " for c in text if not unicodedata.combining(c))
    tokens = {t for t in text.split() if t}
    return frozenset(tokens - _NAME_NOISE) or frozenset(tokens)


def names_similar(a: frozenset[str], b: frozenset[str]) -> bool:
    if not a or not b:
        return False
    return a <= b or b <= a or len(a & b) / len(a | b) >= 0.5


def distance_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    x = math.radians(lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    y = math.radians(lat2 - lat1)
    return 6_371_000 * math.hypot(x, y)


def station_country(src: Source, station_id: int, dim_country: str | None) -> str:
    if src.dataset_id == "ch":
        return UIC_COUNTRY.get(station_id // 100_000, "XX")
    if src.dataset_id == "nl":
        return NL_COUNTRY.get(dim_country or "", dim_country or "XX")
    return dim_country or src.country


def served_stations(src: Source) -> dict[int, dict]:
    """Stations at either end of a published route, with position and country."""
    import duckdb

    static = src.publish_root / "static"
    listed = {s["id"]: s for s in json.loads((static / "stations.json").read_text())}
    served = {sid for pair in json.loads((static / "route_pairs.json").read_text()).values()
              for sid in pair}
    has_country = "country" in {r[0] for r in duckdb.sql(
        f"DESCRIBE SELECT * FROM '{src.dim.as_posix()}'").fetchall()}
    countries = dict(duckdb.sql(
        f"SELECT bpuic, any_value(country) FROM '{src.dim.as_posix()}' GROUP BY 1"
    ).fetchall()) if has_country else {}
    out = {}
    for sid in served:
        s = listed.get(sid)
        if s is None or s.get("lon") is None:
            continue
        out[sid] = {"id": sid, "name": s["name"], "lon": s["lon"], "lat": s["lat"],
                    "country": station_country(src, sid, countries.get(sid))}
    return out


def build_crosswalk(stations: dict[str, dict[int, dict]]) -> dict:
    """Groups of one physical station across datasets.

    Each station takes at most one partner per other dataset, its best match. Pairs are then
    joined transitively, so a station in three datasets is one group.
    """
    parent: dict[tuple[str, int], tuple[str, int]] = {}

    def find(k):
        while parent.setdefault(k, k) != k:
            parent[k] = parent[parent[k]]
            k = parent[k]
        return k

    evidence: dict[tuple[str, int], list[tuple[float, str]]] = {}
    ids = sorted(stations)
    for i, a in enumerate(ids):
        # A 0.02° grid over b keeps this linear; neighbours sit in the 3x3 cells around a.
        for b in ids[i + 1:]:
            grid: dict[tuple[int, int], list[dict]] = {}
            for s in stations[b].values():
                grid.setdefault((int(s["lon"] / 0.02), int(s["lat"] / 0.02)), []).append(s)
            for s in stations[a].values():
                gx, gy = int(s["lon"] / 0.02), int(s["lat"] / 0.02)
                name_a = normalize_name(s["name"])
                best = None
                for dx in (-1, 0, 1):
                    for dy in (-1, 0, 1):
                        for t in grid.get((gx + dx, gy + dy), ()):
                            d = distance_m(s["lon"], s["lat"], t["lon"], t["lat"])
                            name_b = normalize_name(t["name"])
                            if d <= NAME_MATCH_M and names_similar(name_a, name_b):
                                rank, method = 0, "distance+name"
                            elif d <= SAME_NAME_M and name_a == name_b:
                                rank, method = 1, "same-name"
                            elif d <= POINT_M:
                                rank, method = 2, "same-point"
                            else:
                                continue
                            if best is None or (rank, d) < best[:2]:
                                best = (rank, d, method, t)
                if best is not None:
                    rank, d, method, t = best
                    ka, kb = (a, s["id"]), (b, t["id"])
                    parent[find(ka)] = find(kb)
                    evidence.setdefault(ka, []).append((d, method))
                    evidence.setdefault(kb, []).append((d, method))

    groups: dict[tuple[str, int], list[tuple[str, int]]] = {}
    for key in parent:
        groups.setdefault(find(key), []).append(key)
    out = []
    for members in groups.values():
        if len({ds for ds, _ in members}) < 2:
            continue
        members.sort()
        facts = [e for m in members for e in evidence.get(m, [])]
        worst = max(d for d, _ in facts)
        methods = sorted({m for _, m in facts})
        out.append({
            "name": stations[members[0][0]][members[0][1]]["name"],
            "members": [
                {"dataset": ds, "station": sid, "name": stations[ds][sid]["name"],
                 "lon": stations[ds][sid]["lon"], "lat": stations[ds][sid]["lat"],
                 "country": stations[ds][sid]["country"]}
                for ds, sid in members
            ],
            "method": "+".join(methods),
            "max_distance_m": round(worst, 1),
            "confidence": "high" if methods == ["distance+name"] else "medium",
            "review_status": "unreviewed",
            "valid_from": None,
            "valid_to": None,
        })
    out.sort(key=lambda g: (g["members"][0]["dataset"], g["members"][0]["station"]))
    for n, group in enumerate(out, 1):
        group["id"] = n
    return {
        "crosswalk_schema_version": 1,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "rules": {"name_match_m": NAME_MATCH_M, "same_name_m": SAME_NAME_M,
                  "same_point_m": POINT_M},
        "groups": out,
    }


def canonical_map(crosswalk: dict) -> dict[tuple[str, int], int]:
    return {(m["dataset"], m["station"]): g["id"]
            for g in crosswalk["groups"] for m in g["members"]}


# --- journeys -------------------------------------------------------------------------------


def _number_sql(dataset_id: str) -> str:
    """The public train number inside a dataset's trip_id; mirrors the client's parser."""
    if dataset_id == "ch":
        expr = ("CASE WHEN trip_id LIKE '%:sjyid:%' "
                "THEN split_part(regexp_extract(trip_id, '[^:]+$'), '-', 1) "
                "ELSE list_extract(string_split(trip_id, ':'), -2) END")
    else:
        expr = "regexp_extract(regexp_replace(trip_id, ' #[0-9]+$', ''), '([^ ]+)$', 1)"
    # "00275" and "275" are one number; anything not a number cannot be matched.
    return f"nullif(ltrim(regexp_extract({expr}, '^[0-9]+$'), '0'), '')"


def _route_pairs(con, src: Source) -> None:
    """route_id → (from, to) as a table, once per connection."""
    name = f"_rp_{src.dataset_id}"
    if con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = ?",
                   [name]).fetchone()[0]:
        return
    pairs = json.loads((src.publish_root / "static" / "route_pairs.json").read_text())
    con.execute(f"CREATE TEMP TABLE {name} (route_id INTEGER, f BIGINT, t BIGINT)")
    con.executemany(f"INSERT INTO {name} VALUES (?, ?, ?)",
                    [[int(k), v[0], v[1]] for k, v in pairs.items()])


def journey_visits(con, src: Source, day: date, table: str) -> bool:
    """One row per station call of every journey in the day file: (journey, station, t).

    A leg split into route fractions only has a real station where its fraction starts at 0
    or ends at 1.
    """
    legs, journeys = src.legs(day), src.journeys(day)
    if not legs.exists() or not journeys.exists():
        return False
    _route_pairs(con, src)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table} AS
        WITH l AS (
            SELECT journey_id, t_dep, dur, type, delay,
                   CASE WHEN flags & {FLAG_ROUTE_FRACTION} != 0 THEN route_id & 65535
                        ELSE route_id END AS rid,
                   CASE WHEN flags & {FLAG_ROUTE_FRACTION} != 0
                        THEN (route_id >> 16) & 255 ELSE 0 END AS s_byte,
                   CASE WHEN flags & {FLAG_ROUTE_FRACTION} != 0
                        THEN route_id >> 24 ELSE 255 END AS e_byte
            FROM '{legs.as_posix()}'
        ),
        j AS (SELECT journey_id, trip_id, {_number_sql(src.dataset_id)} AS number
              FROM '{journeys.as_posix()}')
        SELECT l.journey_id, j.trip_id, j.number, r.f AS station, l.t_dep AS t, 'dep' AS side,
               l.type, l.delay
        FROM l JOIN _rp_{src.dataset_id} r ON r.route_id = l.rid JOIN j USING (journey_id)
        WHERE l.s_byte = 0
        UNION ALL
        SELECT l.journey_id, j.trip_id, j.number, r.t, l.t_dep + l.dur, 'arr', l.type, l.delay
        FROM l JOIN _rp_{src.dataset_id} r ON r.route_id = l.rid JOIN j USING (journey_id)
        WHERE l.e_byte = 255""")
    return True


def link_day(con, a: Source, b: Source, day: date, stations: dict[str, dict[int, dict]],
             canon: dict[tuple[str, int], int]) -> list[dict]:
    """Journey links between datasets a and b for one UTC day, in both directions."""
    if not (journey_visits(con, a, day, "_va") and journey_visits(con, b, day, "_vb")):
        return []
    rows = {}
    for tag, src in (("a", a), ("b", b)):
        visits = con.execute(f"""
            SELECT journey_id, any_value(trip_id), any_value(number),
                   list(struct_pack(station := station, t := t, side := side,
                                    type := type, delay := delay) ORDER BY t, side)
            FROM _v{tag} WHERE number IS NOT NULL GROUP BY journey_id""").fetchall()
        by_number: dict[str, list] = {}
        for jid, trip, number, calls in visits:
            by_number.setdefault(number, []).append((jid, trip, calls))
        rows[tag] = by_number

    day_number = (day - date(1970, 1, 1)).days
    seen_by: dict[int, set[str]] = {}
    for (ds, _sid), group in canon.items():
        seen_by.setdefault(group, set()).add(ds)
    links = []
    for number in rows["a"].keys() & rows["b"].keys():
        for ja in rows["a"][number]:
            for jb in rows["b"][number]:
                for first, second, fs, ss in ((ja, jb, a, b), (jb, ja, b, a)):
                    link = _judge(first, second, fs, ss, stations, canon, seen_by)
                    # Journey ids are local to a day file, and a bridge extends the first
                    # journey, so it must depart on that journey's day.
                    if link and "bridge" in link and \
                            link["bridge"]["t_dep"] // 86_400 != day_number:
                        continue
                    if link:
                        link["train"] = number
                        links.append(link)
    return links


def _judge(first, second, fs: Source, ss: Source, stations, canon, seen_by) -> dict | None:
    """Is `second` the continuation of `first`? None, or the link."""
    jf, trip_f, calls_f = first
    js, trip_s, calls_s = second
    last = calls_f[-1]
    start = calls_s[0]
    if start["t"] < calls_f[0]["t"]:
        return None  # the continuation cannot start before the train does
    base = {"from": [fs.dataset_id, jf], "to": [ss.dataset_id, js],
            "from_trip": trip_f, "to_trip": trip_s}

    # Shared station at about the same time: the datasets overlap along the border belt.
    for cf in calls_f:
        g = canon.get((fs.dataset_id, cf["station"]))
        if g is None:
            continue
        for cs in calls_s:
            if canon.get((ss.dataset_id, cs["station"])) == g and \
                    abs(cs["t"] - cf["t"]) <= SHARED_STATION_S:
                # Spectating switches when the first journey runs out.
                kind = "handover" if cf is last and cs is start else "overlap"
                if calls_s[-1]["t"] <= last["t"]:
                    return None  # the second ends first: not a continuation
                return base | {"kind": kind, "at": int(max(last["t"], start["t"]))
                               if kind == "handover" else int(last["t"])}

    # A gap across a border that a train could cover.
    st_f = stations[fs.dataset_id].get(last["station"])
    st_s = stations[ss.dataset_id].get(start["station"])
    if st_f is None or st_s is None or last["side"] != "arr" or start["side"] != "dep":
        return None
    # Each end in its own dataset's country: a Swiss journey ending at Mulhouse is not at the
    # Swiss edge.
    if st_f["country"] != fs.country or st_s["country"] != ss.country:
        return None
    # A bridge spans only what the other dataset cannot see. Switzerland serves Waldshut, so a
    # German train ending there that really ran on to Basel would be in the Swiss journey.
    if ss.dataset_id in seen_by.get(canon.get((fs.dataset_id, last["station"])), ()) or \
            fs.dataset_id in seen_by.get(canon.get((ss.dataset_id, start["station"])), ()):
        return None
    gap_s = start["t"] - last["t"]
    dist = distance_m(st_f["lon"], st_f["lat"], st_s["lon"], st_s["lat"])
    if not (BRIDGE_MIN_S <= gap_s <= BRIDGE_MAX_S and dist <= BRIDGE_MAX_M
            and BRIDGE_MIN_SPEED_MS <= dist / gap_s <= BRIDGE_MAX_SPEED_MS):
        return None
    return base | {
        "kind": "bridge", "at": int(start["t"]),
        "bridge": {"from_station": last["station"], "to_station": start["station"],
                   "t_dep": int(last["t"]), "dur": int(gap_s), "type": int(last["type"]),
                   "delay": int(last["delay"]), "distance_m": round(dist)},
    }


def one_per_end(links: list[dict]) -> list[dict]:
    """A journey continues into at most one journey, and is continued from at most one, over
    every dataset at once: the tightest match wins (shared station, then shortest gap). An
    Aachen–Heerlen–Liège train then links Germany → Netherlands → Belgium, not also straight
    from Germany to Belgium across the Dutch section."""
    order = {"handover": 0, "overlap": 1, "bridge": 2}
    links.sort(key=lambda lk: (order[lk["kind"]], lk.get("bridge", {}).get("dur", 0),
                              lk["from"], lk["to"]))
    used_out, used_in, out = set(), set(), []
    for link in links:
        ko = tuple(link["from"])
        ki = tuple(link["to"])
        if ko in used_out or ki in used_in:
            continue
        used_out.add(ko)
        used_in.add(ki)
        out.append(link)
    return sorted(out, key=lambda lk: (lk["at"], lk["from"], lk["to"]))


# --- build ----------------------------------------------------------------------------------


def overlap_days(srcs: dict[str, Source], pairs: list[tuple[str, str]]) -> dict[date, list]:
    days: dict[date, list] = {}
    published = {k: s.days() for k, s in srcs.items()}
    for a, b in pairs:
        for d in published[a] & published[b]:
            days.setdefault(d, []).append((a, b))
    return days


def build(out_root: Path = LINKS_ROOT, first: date | None = None, last: date | None = None,
          log=print) -> dict:
    import duckdb

    t0 = time.monotonic()
    publish = out_root / "publish"
    # Day files are rewritten whole: a day that no longer has links must not keep old ones.
    shutil.rmtree(publish / "days", ignore_errors=True)
    srcs = sources()
    pairs = neighbours(srcs)
    stations = {k: served_stations(s) for k, s in srcs.items()}
    crosswalk = build_crosswalk(stations)
    canon = canonical_map(crosswalk)
    _write(publish / "crosswalk.json", crosswalk)
    log(f"crosswalk: {len(crosswalk['groups'])} groups over {len(pairs)} neighbour pairs")

    days = overlap_days(srcs, pairs)
    if first:
        days = {d: p for d, p in days.items() if d >= first}
    if last:
        days = {d: p for d, p in days.items() if d <= last}
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    per_day: dict[date, list[dict]] = {}
    for n, day in enumerate(sorted(days), 1):
        links = []
        for a, b in days[day]:
            links += link_day(con, srcs[a], srcs[b], day, stations, canon)
        per_day[day] = one_per_end(links)
        if n % 100 == 0:
            log(f"  {n}/{len(days)} days")

    verdicts = judge_bridge_pairs(per_day, edge_distances(per_day, stations))
    bridge_pairs: dict[tuple, int] = {}
    counts: dict[str, int] = {}
    for day, links in per_day.items():
        kept = []
        for link in links:
            if "bridge" in link:
                key = bridge_key(link)
                verdict = verdicts[key]
                if not verdict["accepted"]:
                    counts["bridge_rejected_pair"] = counts.get("bridge_rejected_pair", 0) + 1
                    continue
                # An accepted pair can still carry a coincidence: IC 2872 → L 2872 takes 78
                # minutes on the 40-minute Rotterdam → Antwerp pair.
                gap = abs(link["bridge"]["dur"] - verdict["median_dur_s"])
                if gap > max(600, 0.35 * verdict["median_dur_s"]):
                    counts["bridge_rejected_duration"] = \
                        counts.get("bridge_rejected_duration", 0) + 1
                    continue
                bridge_pairs.setdefault(key, 0)
            counts[link["kind"]] = counts.get(link["kind"], 0) + 1
            kept.append(link)
        per_day[day] = kept

    # Bridge route ids are assigned once over the whole build, in a stable order, and the day
    # files and routes.bin are always written together.
    for rid, key in enumerate(sorted(bridge_pairs), 1):
        bridge_pairs[key] = rid
    day_bytes = {}
    for day, links in per_day.items():
        for link in links:
            if "bridge" in link:
                link["bridge"]["route"] = bridge_pairs[bridge_key(link)]
        path = publish / "days" / f"{day:%Y/%m/%d}.json"
        _write(path, {"day": day.isoformat(), "links": links}, compact=True)
        day_bytes[day.isoformat()] = path.stat().st_size

    write_bridge_inputs(out_root, bridge_pairs, stations)
    manifest = {
        "links_schema_version": LINKS_SCHEMA_VERSION,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "pairs": [list(p) for p in pairs],
        "days": dict(sorted(day_bytes.items())),
        "crosswalk": "crosswalk.json",
        "routes": "static/routes.bin",
        "dedup_rule": DEDUP_RULE,
        "dedup_window_s": DEDUP_WINDOW_S,
        "evidence_rank": EVIDENCE_RANK,
        "link_rules": {
            "shared_station_s": SHARED_STATION_S, "handover_max_s": HANDOVER_MAX_S,
            "bridge_max_m": BRIDGE_MAX_M, "bridge_min_s": BRIDGE_MIN_S,
            "bridge_max_s": BRIDGE_MAX_S, "bridge_max_speed_ms": BRIDGE_MAX_SPEED_MS,
        },
        "bridge_semantics": "interpolated",
        "counts": counts,
        "bridge_routes": len(bridge_pairs),
        "bridge_pairs": [
            {"route": bridge_pairs.get(key), "from": [key[0], key[1]], "to": [key[2], key[3]]}
            | verdict
            for key, verdict in sorted(verdicts.items(), key=lambda kv: -kv[1]["trains"])
        ],
    }
    _write(publish / "manifest.json", manifest)
    log(f"links: {len(per_day)} days {counts}, {len(bridge_pairs)} bridge routes "
        f"in {time.monotonic() - t0:.0f}s")
    return manifest


def bridge_key(link: dict) -> tuple:
    br = link["bridge"]
    return (link["from"][0], br["from_station"], link["to"][0], br["to_station"])


def _within(dur: float, median: float) -> bool:
    return abs(dur - median) <= max(300.0, BRIDGE_DUR_TOLERANCE * median)


def judge_bridge_pairs(per_day: dict[date, list[dict]],
                       edge_m: dict[tuple[str, int, str], float]) -> dict[tuple, dict]:
    """Accept or reject every bridge station pair on its record over the whole build.

    edge_m: (dataset, station, other dataset) → metres to the nearest station the other
    dataset serves.
    """
    import statistics

    days: dict[tuple, set] = {}
    durs: dict[tuple, list[int]] = {}
    numbers: dict[tuple, set] = {}
    for day, links in per_day.items():
        for link in links:
            if "bridge" in link:
                key = bridge_key(link)
                days.setdefault(key, set()).add(day)
                durs.setdefault(key, []).append(link["bridge"]["dur"])
                numbers.setdefault(key, set()).add(link["train"])
    out = {}
    for key in days:
        reverse = (key[2], key[3], key[0], key[1])
        n = len(days[key])
        trains = len(durs[key])
        median = statistics.median(durs[key])
        consistent = sum(_within(d, median) for d in durs[key]) / trains
        edge = max(edge_m[(key[0], key[1], key[2])], edge_m[(key[2], key[3], key[0])])
        both_ways = len(days.get(reverse, ())) >= BRIDGE_MIN_DAYS
        verdict = {
            "days": n, "trains": trains, "per_day": round(trains / n, 2),
            "numbers": len(numbers[key]), "both_ways": both_ways,
            "median_dur_s": median, "consistent_share": round(consistent, 2),
            "edge_m": round(edge),
        }
        verdict["accepted"] = (
            n >= BRIDGE_MIN_DAYS
            and (both_ways or trains / n >= BRIDGE_MIN_PER_DAY)
            and len(numbers[key]) >= (BRIDGE_MIN_NUMBERS if both_ways
                                      else BRIDGE_MIN_NUMBERS_ONE_WAY)
            and edge <= BRIDGE_EDGE_M
            and consistent >= BRIDGE_CONSISTENT_SHARE
        )
        out[key] = verdict
    return out


def edge_distances(per_day: dict[date, list[dict]], stations: dict[str, dict[int, dict]]
                   ) -> dict[tuple[str, int, str], float]:
    """Metres from every bridge end to the nearest station the other side's dataset serves."""
    wanted = set()
    for links in per_day.values():
        for link in links:
            if "bridge" in link:
                br = link["bridge"]
                wanted.add((link["from"][0], br["from_station"], link["to"][0]))
                wanted.add((link["to"][0], br["to_station"], link["from"][0]))
    out = {}
    for ds, sid, other in wanted:
        s = stations[ds][sid]
        out[(ds, sid, other)] = min(
            distance_m(s["lon"], s["lat"], t["lon"], t["lat"]) for t in stations[other].values()
        )
    return out


def write_bridge_inputs(out_root: Path, bridge_pairs: dict[tuple, int],
                        stations: dict[str, dict[int, dict]]) -> None:
    """The geometry job's inputs for the bridges: a station_pairs table and a station dim,
    with station ids made unique across datasets (GEOMETRY_ID_PREFIX)."""
    import duckdb

    def gid(ds: str, sid: int) -> int:
        return GEOMETRY_ID_PREFIX[ds] * 1_000_000_000 + sid

    work = out_root / "geometry"  # beside publish/, never uploaded
    work.mkdir(parents=True, exist_ok=True)
    db = work / "bridges.duckdb"
    if db.exists():
        db.unlink()
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE station_pairs (route_id INTEGER, from_bpuic BIGINT, "
                "to_bpuic BIGINT)")
    con.executemany("INSERT INTO station_pairs VALUES (?, ?, ?)",
                    [[rid, gid(a, fa), gid(b, tb)] for (a, fa, b, tb), rid in bridge_pairs.items()])
    used = {(a, fa) for a, fa, _, _ in bridge_pairs} | {(b, tb) for _, _, b, tb in bridge_pairs}
    con.execute("CREATE TABLE dim (bpuic BIGINT, name VARCHAR, lon DOUBLE, lat DOUBLE, "
                "valid_from DATE, valid_to DATE)")
    con.executemany(
        "INSERT INTO dim VALUES (?, ?, ?, ?, DATE '1900-01-01', DATE '9999-12-31')",
        [[gid(ds, sid), stations[ds][sid]["name"], stations[ds][sid]["lon"],
          stations[ds][sid]["lat"]] for ds, sid in sorted(used)],
    )
    con.execute(f"COPY dim TO '{(work / 'dim.parquet').as_posix()}' (FORMAT PARQUET)")
    con.close()


def _write(path: Path, obj: dict, compact: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, separators=(",", ":")) if compact
                    else json.dumps(obj, indent=1, ensure_ascii=False))

