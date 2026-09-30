"""Italy adapter: TrainStats' daily JSON archive of ViaggiaTreno → normalized stop events.

TrainStats (trainstats.altervista.org) polls ViaggiaTreno, the RFI/Trenitalia passenger
information system, through each day and stores every train it tracked with its final state:
per stop, the scheduled arrival/departure (epoch seconds, UTC) and the reported deviation in
whole minutes. One JSON file per service day, organized by the day a train departs its origin
(see docs/data-notes-it.md). it_fetch downloads them; they never go to R2.

Two stages, like Great Britain:

  extract   one day's JSON → extract/<day>.{trains,stops}.parquet plus the day's census row.
            Every key is checked against the known schema, so a format change fails loudly.
  stage     extracts → stg_stops, through the ViaggiaTreno station dictionary.

Normalization rules:

  - A delay value is a number of minutes, or a marker: N (the side does not exist — origin
    arrival, terminus departure), S (the stop was cancelled), n.d. (not detected). Only a number
    becomes a reconstructed time: scheduled + delay. n.d. falls back to the timetable, flagged.
  - A cancelled stop (S) keeps its row with no times, so the leg builder never bridges a
    part-cancelled train across the gap (stops.py's not_run verdict). A side marked N has no
    event, whatever time the source prints for it.
  - A train whose train-level delay is X was cancelled outright: no legs, counted per day.
  - Before 2023-11-04 TrainStats recorded every FerrovieNord station with delay 0. Stations
    whose delays in that period are almost all exactly zero are treated as not detected there.
  - A delay outside [-30 min, 12 h] is a ViaggiaTreno artefact: treated as not detected.
  - Station names are matched to ViaggiaTreno station codes by exact name, then through
    pipeline/data/it_station_aliases.csv (renamed stations, FerrovieNord's old names).
"""

import csv
import gzip
import json
import re
from datetime import date
from pathlib import Path

from berg_pipeline import ingest
from berg_pipeline.europe import it_fetch
from berg_pipeline.europe.config import ITALY as CFG

VT = "http://www.viaggiatreno.it/infomobilita/resteasy/viaggiatreno"
ALIASES_CSV = Path(__file__).resolve().parents[3] / "data" / "it_station_aliases.csv"
FIGSHARE_STATIONS = "Train_station_locations_data.csv"
OSM_STATIONS = "osm_stations.json"
OVERPASS = "https://overpass-api.de/api/interpreter"

# Every key the archive's train and stop objects use (github.com/emanu37429/trainstats). A key
# outside these fails the extract: an unknown field is a format change to read, not ignore.
TRAIN_KEYS = frozenset(
    "_id n p rp a ra c sub op oa fr cn dl oo od sep sea ope oae oaz opz pr".split()
)
STOP_KEYS = frozenset("n ra rp oa op br bp".split())
TOP_KEYS = frozenset("giorno timeZone riassunto avvisiRFI avvisiTI treni".split())

TRAIN_COLS = (
    "service_day tidx source_id number c sub p a rp ra op oa oo od dl cn sep sea ope oae n_stops"
).split()
STOP_COLS = "service_day tidx seq name s_arr s_dep ra rp".split()

# TrainStats fixed FerrovieNord delay collection on this day (changelog 03/11/2023).
FN_FIX_DAY = date(2023, 11, 4)
FN_ZERO_SHARE = 0.99
DELAY_MIN_MIN, DELAY_MAX_MIN = -30, 720

# Category codes as the archive writes them. Frecce are "" (or ES*) with sub FR/FA/FB.
CATEGORY_NAMES = {
    "REG": "Regionale",
    "MET": "Metropolitano",
    "IC": "InterCity",
    "ICN": "InterCity Notte",
    "EC": "EuroCity",
    "EN": "EuroNight",
    "FR": "Frecciarossa",
    "FA": "Frecciargento",
    "FB": "Frecciabianca",
    "EXP": "Espresso",
    "IR": "InterRegionale",
    "NCL": "Non classificato",
}


def extract_dir() -> Path:
    return it_fetch.raw_root(CFG) / "extract"


def _is_int(v) -> bool:
    return isinstance(v, str) and re.fullmatch(r"-?\d+", v) is not None


def extract_day(day: date) -> dict:
    """One day's JSON → extract parquet pair; returns the day's census row. Cached: an
    existing pair is trusted and its census row re-read."""
    import duckdb

    trains_out = extract_dir() / f"{day}.trains.parquet"
    stops_out = extract_dir() / f"{day}.stops.parquet"
    census_out = extract_dir() / f"{day}.census.json"
    if trains_out.exists() and stops_out.exists() and census_out.exists():
        return json.loads(census_out.read_text())
    path = it_fetch.day_file(CFG, day)
    if path is None:
        raise RuntimeError(f"{day}: no raw file")
    raw = it_fetch.read_day(path)
    unknown = set(raw) - TOP_KEYS
    if unknown:
        raise RuntimeError(f"{day}: unknown top-level keys {sorted(unknown)}")
    stated = raw.get("giorno")
    if stated != f"{day:%d/%m/%Y}":
        raise RuntimeError(f"{day}: file says giorno={stated!r}")

    extract_dir().mkdir(parents=True, exist_ok=True)
    trains_csv, stops_csv = trains_out.with_suffix(".csv.gz"), stops_out.with_suffix(".csv.gz")
    c = {
        "day": day.isoformat(),
        "source": path.name,
        "compressed_bytes": path.stat().st_size,
        "summary_schema": "v2" if "ritardoFrecce" in raw.get("riassunto", {}) else "v1",
        # Trenitalia's own count of trains that ran that day, from TrainStats' summary: the
        # denominator for how much of the day TrainStats managed to collect.
        "circulated": (raw.get("riassunto") or {}).get("treniCircolati"),
        "platforms": False,
        "trains": 0,
        "trains_with_stops": 0,
        "calls": 0,
        "arr_numeric": 0,
        "dep_numeric": 0,
        "arr_nd": 0,
        "dep_nd": 0,
        "cancelled_trains": 0,
        "cancelled_stops": 0,
        "part_cancelled_trains": 0,
        "rerouted_trains": 0,
        "number_changes": 0,
        "international": 0,
        "duplicate_ids": 0,
    }
    stations, categories, ids = set(), {}, set()
    with (
        gzip.open(trains_csv, "wt", newline="", compresslevel=1) as ft,
        gzip.open(stops_csv, "wt", newline="", compresslevel=1) as fs,
    ):
        wt, ws = csv.writer(ft), csv.writer(fs)
        wt.writerow(TRAIN_COLS)
        ws.writerow(STOP_COLS)
        for tidx, t in enumerate(raw["treni"]):
            unknown = set(t) - TRAIN_KEYS
            if unknown:
                raise RuntimeError(f"{day}: unknown train keys {sorted(unknown)} in {t.get('_id')}")
            fr = t.get("fr") or []
            c["trains"] += 1
            c["trains_with_stops"] += bool(fr)
            c["calls"] += len(fr)
            cancelled = t.get("rp") == "X" or t.get("ra") == "X"
            c["cancelled_trains"] += cancelled
            c["rerouted_trains"] += bool(t.get("dl") or t.get("oo") or t.get("od"))
            c["number_changes"] += bool(t.get("cn"))
            c["international"] += bool(t.get("sep") or t.get("sea"))
            sid = t.get("_id")
            if sid in ids:
                c["duplicate_ids"] += 1
            ids.add(sid)
            cat = t.get("c") or ""
            categories[f"{cat}/{t.get('sub') or ''}"] = (
                categories.get(f"{cat}/{t.get('sub') or ''}", 0) + 1
            )
            part = False
            for seq, s in enumerate(fr):
                unknown = set(s) - STOP_KEYS
                if unknown:
                    raise RuntimeError(f"{day}: unknown stop keys {sorted(unknown)} in {sid}")
                if "br" in s or "bp" in s:
                    c["platforms"] = True
                ra, rp = s.get("ra"), s.get("rp")
                c["arr_numeric"] += _is_int(ra)
                c["dep_numeric"] += _is_int(rp)
                c["arr_nd"] += ra == "n.d."
                c["dep_nd"] += rp == "n.d."
                if ra == "S" or rp == "S":
                    c["cancelled_stops"] += 1
                    part = True
                for v in (ra, rp):
                    if not (_is_int(v) or v in ("N", "S", "n.d.")):
                        raise RuntimeError(f"{day}: unknown delay marker {v!r} in {sid}")
                stations.add(s["n"])
                ws.writerow([day, tidx, seq, s["n"], s.get("oa"), s.get("op"), ra, rp])
            c["part_cancelled_trains"] += part and not cancelled
            wt.writerow(
                [day, tidx, sid, t.get("n"), t.get("c"), t.get("sub"), t.get("p"), t.get("a")]
                + [t.get(k) for k in ("rp", "ra", "op", "oa", "oo", "od", "dl", "cn")]
                + [t.get(k) for k in ("sep", "sea", "ope", "oae")]
                + [len(fr)]
            )
    c["unique_stations"] = len(stations)
    c["categories"] = dict(sorted(categories.items()))

    con = duckdb.connect()
    for tmp_csv, out in ((trains_csv, trains_out), (stops_csv, stops_out)):
        tmp = out.with_suffix(".tmp")
        con.execute(f"""
            COPY (SELECT * FROM read_csv('{tmp_csv.as_posix()}', header=true, all_varchar=true,
                                   delim=',', quote='"', escape='"'))
            TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)""")
        tmp.rename(out)
        tmp_csv.unlink()
    census_out.write_text(json.dumps(c))
    return c


# --- stations -----------------------------------------------------------------------------


def station_dir() -> Path:
    return CFG.raw_dir / "viaggiatreno"


def fetch_stations(force: bool = False) -> None:
    """ViaggiaTreno's station registry (code, names, coordinates, 23 region lists), its name
    index (autocompletaStazione, one letter or digit at a time), and OSM's railway stations in
    Italy for checking both. One-off snapshots of static metadata; dates in
    docs/data-notes-it.md."""
    import httpx

    out = station_dir()
    out.mkdir(parents=True, exist_ok=True)
    with httpx.Client(headers={"User-Agent": it_fetch.USER_AGENT}, timeout=60) as client:
        for region in range(23):
            p = out / f"elencoStazioni_{region}.json"
            if force or not p.exists():
                r = client.get(f"{VT}/elencoStazioni/{region}")
                r.raise_for_status()
                p.write_bytes(r.content)
        for ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789":
            p = out / f"autocompleta_{ch}.txt"
            if force or not p.exists():
                r = client.get(f"{VT}/autocompletaStazione/{ch}")
                r.raise_for_status()
                p.write_bytes(r.content)
        p = out / OSM_STATIONS
        if force or not p.exists():
            query = (
                '[out:json][timeout:180];area["ISO3166-1"="IT"][admin_level=2]->.it;'
                '(node["railway"~"^(station|halt|stop)$"](area.it);'
                'node["public_transport"="station"]["train"="yes"](area.it););out tags center;'
            )
            r = client.post(OVERPASS, data={"data": query}, timeout=300)
            r.raise_for_status()
            p.write_bytes(r.content)


def station_id(code: str) -> int:
    """Station code → dataset-local id. ViaggiaTreno S-codes keep their number (S01700 →
    1700); the registry's F-codes, FerrovieNord's N-codes (from the Figshare list) and the
    aliases' own X-codes for stations ViaggiaTreno does not list get disjoint ranges."""
    prefix, num = code[0], int(code[1:])
    return {"S": 0, "F": 1_000_000, "N": 2_000_000, "X": 3_000_000}[prefix] + num


def normalize(name: str) -> str:
    """The spelling differences between the archive's eras and the station lists: case, the
    backtick the pre-2025 names use for an apostrophe, and spaces around '.' and '-'."""
    s = name.upper().replace("`", "'").strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*-\s*", "-", s)
    return re.sub(r"\.\s+", ".", s)


# Junctions and operational points (bivio, posto movimento, posto di comunicazione,
# deviatoio) appear as calls of diverted trains. They are not stations: the call is dropped
# and the train's legs join its real stops on either side, which it did run between.
OPERATIONAL_POINT = re.compile(
    r"^(\d*°?`?BIVIO|BV/|PM\b|P\.M\.|PC\b|PP\b|POSTO MOV|DEV\b|DEV\.|DEVIATOIO|LIMITE FS|"
    r"POSTO COMUNICAZ|POSTO DI MOV|TRIPLO BIVIO|CONFLUENZA|\d+\?IV|.*SMISTAM|.*\bPARCO\b|"
    r".*RACCORDO|.*FASCIO MERCI|.*CABINA|.*DEVIATOIO|C\.C\.\d)",
    re.IGNORECASE,
)


def load_registry() -> dict[str, dict]:
    """ViaggiaTreno's station registry by code."""
    reg: dict[str, dict] = {}
    for p in sorted(station_dir().glob("elencoStazioni_*.json")):
        for s in json.loads(p.read_text(encoding="utf-8")):
            # A station appears again in each neighbouring region's list, flagged esterno.
            if s["codStazione"] not in reg or reg[s["codStazione"]].get("esterno"):
                reg[s["codStazione"]] = s
    return reg


def load_figshare() -> dict[str, dict]:
    """The 2024 station list published with the Figshare dataset (CC BY 4.0): codes,
    old-style names and coordinates, including FerrovieNord's N-codes."""
    with open(CFG.raw_dir / FIGSHARE_STATIONS, newline="", encoding="latin-1") as fh:
        return {r["station_id"]: r for r in csv.DictReader(fh)}


def squash(name: str) -> str:
    """A name reduced to letters and digits, for comparing against OSM's spelling."""
    import unicodedata

    s = unicodedata.normalize("NFKD", name.replace("`", "'")).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def load_osm() -> dict[str, list[tuple[float, float]]]:
    """OSM railway stations and halts in Italy (fetch_stations' Overpass snapshot), by every
    name they carry."""
    out: dict[str, list[tuple[float, float]]] = {}
    for e in json.loads((station_dir() / OSM_STATIONS).read_text(encoding="utf-8"))["elements"]:
        tags = e.get("tags", {})
        for k in ("name", "official_name", "alt_name", "old_name", "short_name", "name:it"):
            for v in tags.get(k, "").split(";"):
                if v.strip():
                    out.setdefault(squash(v), []).append((e["lat"], e["lon"]))
    return out


# Two points of one station agree when within this distance; farther apart a station code is
# either misplaced in a list or reused for another place.
SAME_PLACE_KM = 3.0
# A stop lies between its neighbours when going through it is at most this much longer than
# going straight; a stop with one placed neighbour must be within CONTEXT_KM of it.
DETOUR_RATIO, DETOUR_KM = 1.5, 20.0
CONTEXT_KM = 120.0
# Italy with its border belt. The station lists put ~80 stations near 0°N 0°E.
_BOX = (35.3, 6.5, 47.2, 18.6)


def _valid(pt) -> bool:
    return pt is not None and _BOX[0] < pt[0] < _BOX[2] and _BOX[1] < pt[1] < _BOX[3]


def _km(a: tuple[float, float], b: tuple[float, float]) -> float:
    import math

    dlat = (a[0] - b[0]) * 111.2
    dlon = (a[1] - b[1]) * 111.2 * math.cos(math.radians(a[0]))
    return math.hypot(dlat, dlon)


LAYERS = (
    "alias",
    "index",
    "registry_long",
    "registry_short",
    "squashed",
    "figshare",
    "figshare_short",
)


def _name_layers(reg: dict, fig: dict) -> list[tuple[str, dict[str, set[str]]]]:
    """(layer, normalized name → codes), in priority order: the manual aliases, ViaggiaTreno's
    current name index, the registry's long then short names (the pre-2025-03-27 spelling),
    then the 2024 Figshare list."""
    alias: dict[str, set[str]] = {}
    with open(ALIASES_CSV, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            alias.setdefault(normalize(r["name"]), set()).add(r["code"])
    index: dict[str, set[str]] = {}
    for p in sorted(station_dir().glob("autocompleta_*.txt")):
        for line in p.read_text(encoding="utf-8").splitlines():
            if "|" in line:
                name, code = line.rsplit("|", 1)
                index.setdefault(normalize(name), set()).add(code)
    layers = [("alias", alias), ("index", index)]
    for layer, key in (("registry_long", "nomeLungo"), ("registry_short", "nomeBreve")):
        d: dict[str, set[str]] = {}
        for code, st in reg.items():
            if (st.get("localita") or {}).get(key):
                d.setdefault(normalize(st["localita"][key]), set()).add(code)
        layers.append((layer, d))
    # The current names again, punctuation-blind ("COCQUIO-TREVISAGO" = "COCQUIO TREVISAGO"),
    # before the Figshare list's FerrovieNord duplicates (N-codes) get a chance.
    squashed: dict[str, set[str]] = {}
    for _, d in layers[1:]:
        for n, codes in d.items():
            squashed.setdefault(squash(n), set()).update(codes)
    layers.append(("squashed", squashed))
    for layer, key in (("figshare", "name"), ("figshare_short", "name_short")):
        d = {}
        for code, r in fig.items():
            d.setdefault(normalize(r[key]), set()).add(code)
        layers.append((layer, d))
    return layers


def resolve_stations(
    names: dict[str, int], context: list[tuple[str | None, str, str | None, int]]
) -> list[dict]:
    """Every stop name in the archive (with its call count) → a station, or a verdict.

    Codes are not identities on their own: ViaggiaTreno reuses them (S00193 was Palermo's
    EMS-La Malfa in 2024 and is Torino Orbassano S.Luigi now; PERCA and BRUNICO NORD swapped
    theirs), and both station lists misplace some stations, by kilometres or onto 0°N 0°E. So
    each name gets a code AND a point, and a point is trusted only when two independent
    sources agree within SAME_PLACE_KM:

      code        the first name layer that knows the name (_name_layers).
      confirmed   the list point (from the list that matched the name) has an OSM station
                  of any of the code's spellings within reach (OSM's point is used), or the
                  registry and the Figshare list agree; or a manual alias.
      unconfirmed otherwise: OSM's point for the name when the list point is invalid and
                  OSM's matches agree with each other, else the list point.

    Every point is then checked against the trains themselves (`context`: each stop name
    with the names before and after it, and a count): a stop must lie roughly between its
    neighbours, or within CONTEXT_KM of its only placed one, in at least half its calls. A
    confirmed point that fails is demoted (context_conflict); an unconfirmed name takes the
    first candidate that passes — OSM's point for its own spelling, OSM's for the code's
    other spellings, then the lists' (context_confirmed). Otherwise it is unresolved.

    Names sharing a code are one station unless each is confirmed at a different place; then
    the second place is the code reused and gets the reuse range. An unconfirmed name whose
    code has a confirmed place takes that place. A name with neither code nor point is
    unmatched (or an operational point), and its calls become unmatched_station legs.
    """
    reg, fig, osm = load_registry(), load_figshare(), load_osm()
    layers = _name_layers(reg, fig)
    alias_pts: dict[str, tuple[tuple[float, float], str]] = {}
    with open(ALIASES_CSV, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r["lat"]:
                alias_pts[normalize(r["name"])] = ((float(r["lat"]), float(r["lon"])), r["display"])
    index_names: dict[str, set[str]] = {}
    for lyr, d in layers:
        if lyr == "index":
            for n, codes in d.items():
                for c in codes:
                    index_names.setdefault(c, set()).add(n)

    def pt_of(d, c):
        r = d.get(c)
        if not r or not r.get("lat"):
            return None
        p = (float(r["lat"]), float(r["lon"]))
        return p if _valid(p) else None

    def spellings(c) -> set[str]:
        out = set(index_names.get(c, ()))
        loc = (reg.get(c) or {}).get("localita") or {}
        out |= {loc[k] for k in ("nomeLungo", "nomeBreve") if loc.get(k)}
        if c in fig:
            out |= {fig[c]["name"], fig[c]["name_short"]}
        return {squash(n) for n in out}

    rows = []
    for name, calls in sorted(names.items()):
        key = normalize(name)
        hit = next(
            (
                (lyr, d[k])
                for lyr, d in layers
                for k in [squash(name) if lyr == "squashed" else key]
                if k in d
            ),
            None,
        )
        row = {
            "name": name,
            "calls": calls,
            "code": None,
            "layer": None,
            "lat": None,
            "lon": None,
            "status": None,
            "list_km": None,
            "display": None,
            "confirmed": False,
            "station_id": None,
        }
        rows.append(row)
        if hit is None:
            row["status"] = "operational" if OPERATIONAL_POINT.match(name) else "unmatched"
            continue
        layer, codes = hit
        if key in alias_pts:
            (pt, display), code = alias_pts[key], sorted(codes)[0]
            row.update(
                code=code,
                layer=layer,
                lat=pt[0],
                lon=pt[1],
                status="alias",
                display=display or None,
                confirmed=True,
            )
            continue
        # One name, several codes (a station rebuilt nearby under a new code): the lowest
        # code whose points agree with the others.
        code = sorted(codes)[0]
        figshare = layer.startswith("figshare")
        list_pt = pt_of(fig, code) if figshare else (pt_of(reg, code) or pt_of(fig, code))
        row.update(code=code, layer=layer)
        near = sorted({p for sq in spellings(code) | {squash(name)} for p in osm.get(sq, [])})
        own = sorted(set(osm.get(squash(name), [])))
        other = pt_of(reg, code) if figshare else pt_of(fig, code)
        row["_cands"] = list(dict.fromkeys(own + near + [p for p in (list_pt, other) if p]))
        if list_pt and near:
            best = min(near, key=lambda p: _km(p, list_pt))
            row["list_km"] = round(_km(best, list_pt), 2)
            if row["list_km"] <= SAME_PLACE_KM:
                row.update(lat=best[0], lon=best[1], status="osm_confirmed", confirmed=True)
                continue
        if list_pt and other and _km(list_pt, other) <= SAME_PLACE_KM and not near:
            row.update(lat=list_pt[0], lon=list_pt[1], status="lists_agree", confirmed=True)
            continue
        row["status"] = "unresolved"

    # Context: the trains themselves. A stop must lie roughly between the stops before and
    # after it. Confirmed points that fail are demoted (a name the index gives the wrong
    # place: ACQUAVIVA); unconfirmed names take their first candidate that passes.
    point = {r["name"]: (r["lat"], r["lon"]) for r in rows if r["confirmed"]}
    ctx: dict[str, list[tuple[str | None, str | None, int]]] = {}
    for prev, name, nxt, n in context:
        ctx.setdefault(name, []).append((prev, nxt, n))

    def check(name: str, c: tuple[float, float], pts: dict) -> tuple[bool | None, float]:
        """(passes, mean excess km of the failing calls). None: no placed neighbour."""
        ok = total = 0
        excess = 0.0
        for prev, nxt, n in ctx.get(name, ()):
            a, b = pts.get(prev), pts.get(nxt)
            if a and b:
                over = _km(a, c) + _km(c, b) - (DETOUR_RATIO * _km(a, b) + DETOUR_KM)
            elif a or b:
                over = _km(a or b, c) - CONTEXT_KM
            else:
                continue
            ok, total = ok + n * (over <= 0), total + n
            excess += n * max(0.0, over)
        if not total:
            return None, 0.0
        return ok >= 0.5 * total, excess / total

    def plausible(name: str, c: tuple[float, float], pts: dict) -> bool | None:
        return check(name, c, pts)[0]

    # Worst offender first: one misplaced station makes its correct neighbours look wrong
    # too, until it is removed.
    while True:
        worst, worst_excess = None, 0.0
        for r in rows:
            if r["confirmed"] and r["status"] != "alias":
                ok, excess = check(r["name"], (r["lat"], r["lon"]), point)
                if ok is False and (worst is None or excess > worst_excess):
                    worst, worst_excess = r, excess
        if worst is None:
            break
        failed = (worst["lat"], worst["lon"])
        worst.update(status="context_conflict", confirmed=False, lat=None, lon=None)
        worst["_cands"] = [c for c in worst.get("_cands", []) if _km(c, failed) > SAME_PLACE_KM]
        point.pop(worst["name"], None)
    for r in rows:
        if r["confirmed"] or not r.get("_cands"):
            continue
        for c in r["_cands"]:
            if plausible(r["name"], c, point):
                r.update(lat=c[0], lon=c[1], status="context_confirmed", confirmed=True)
                break
    for r in rows:
        r.pop("_cands", None)

    # Identity: confirmed places of one code; the first keeps the code's number.
    rank = {lyr: i for i, lyr in enumerate(LAYERS)}
    by_code: dict[str, list[dict]] = {}
    for r in rows:
        if r["lat"] is not None:
            by_code.setdefault(r["code"], []).append(r)
    for code, rs in sorted(by_code.items()):
        rs.sort(key=lambda r: (not r["confirmed"], rank[r["layer"]], -r["calls"], r["name"]))
        homes: list[tuple[float, float]] = []
        for r in rs:
            pt = (r["lat"], r["lon"])
            k = next((i for i, h in enumerate(homes) if _km(h, pt) <= SAME_PLACE_KM), None)
            if k is None and not r["confirmed"] and homes:
                # A misplaced spelling of a station confirmed elsewhere.
                k = min(range(len(homes)), key=lambda i: _km(homes[i], pt))
                r.update(lat=homes[k][0], lon=homes[k][1], status=r["status"] + "_adopted")
            if k is None:
                homes.append(pt)
                k = len(homes) - 1
            if k > 1:
                raise RuntimeError(f"code {code} used for three places: {[x['name'] for x in rs]}")
            r["station_id"] = station_id(code) + (REUSED_OFFSET if k else 0)
            if r["display"] is None:
                # The registry's short name, when the name is one of the code's current ones.
                loc = (reg.get(code) or {}).get("localita") or {}
                current = k == 0 and not r["layer"].startswith("figshare") and loc.get("nomeBreve")
                r["display"] = (current or name_title(r["name"])).strip()
    # FerrovieNord's own codes (N, from the Figshare list) name stations ViaggiaTreno also
    # lists under an S-code: the old "COMO NORD CAMERLATA" is today's COMO CAMERLATA (FNM).
    s_places = [r for r in rows if r["station_id"] is not None and r["code"][0] == "S"]
    for r in rows:
        if r["station_id"] is not None and r["code"][0] == "N":
            pt = (r["lat"], r["lon"])
            twin = min(s_places, key=lambda x: _km(pt, (x["lat"], x["lon"])), default=None)
            if twin and _km(pt, (twin["lat"], twin["lon"])) <= FN_TWIN_KM:
                r.update(
                    station_id=twin["station_id"],
                    lat=twin["lat"],
                    lon=twin["lon"],
                    display=twin["display"],
                    code=twin["code"],
                )
    return rows


# An N-coded FerrovieNord station is its S-coded twin when this close.
FN_TWIN_KM = 0.5
# A reused code's second place.
REUSED_OFFSET = 4_000_000

# The station lists have no country field. Every station outside Italy that the archive's
# trains call at, by code.
FOREIGN_STATIONS = {
    "S01301": "CH",  # Chiasso
    "S19919": "CH",  # Stabio
    "S00300": "CH",  # Bellinzona
    "S03001": "CH",  # Zürich Altstetten
    "S75648": "FR",  # Menton
    "S00201": "FR",  # Modane
    "S00190": "FR",  # Modane Fourneaux
    "S01000": "SI",  # Sežana
    "S03303": "SI",  # Nova Gorica
    "S13143": "CH",  # Mendrisio
    "S13144": "CH",  # Balerna
    "S00622": "FR",  # Breil-sur-Roya
    "S00621": "FR",  # Fontan-Saorge
    "S00620": "FR",  # Saint-Dalmas-de-Tende
    "S00619": "FR",  # Tende
    "S00634": "FR",  # La Brigue
}

REVIEW_COLS = "name calls code layer station_id display lat lon status list_km".split()


def write_stations(days: list[date]) -> dict:
    """Resolve every stop name of these days' extracts → dim_station, the name map
    stage_days joins on, and station_review.csv (every decision, for review)."""
    import duckdb

    con = duckdb.connect()
    limit_memory(con)
    con.execute(f"""
        CREATE VIEW _s AS SELECT service_day, CAST(tidx AS INTEGER) AS tidx,
                                 CAST(seq AS INTEGER) AS seq, name
        FROM read_parquet({_files(days, "stops")})""")
    names = dict(con.execute("SELECT name, count(*) FROM _s GROUP BY 1").fetchall())
    context = con.execute("""
        WITH t AS (
            SELECT lag(name) OVER w AS prev, name, lead(name) OVER w AS nxt FROM _s
            WINDOW w AS (PARTITION BY service_day, tidx ORDER BY seq)
        )
        SELECT prev, name, nxt, count(*) FROM t GROUP BY ALL""").fetchall()
    con.execute("DROP VIEW _s")
    rows = resolve_stations(names, context)
    with open(CFG.root / "station_review.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, REVIEW_COLS, extrasaction="ignore")
        w.writeheader()
        for r in sorted(rows, key=lambda r: -r["calls"]):
            w.writerow(r)
    con.execute(
        "CREATE TABLE r (name VARCHAR, code VARCHAR, station_id BIGINT, display VARCHAR, "
        "lat DOUBLE, lon DOUBLE, status VARCHAR, calls BIGINT)"
    )
    con.executemany(
        "INSERT INTO r VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                r["name"],
                r["code"],
                r["station_id"],
                r["display"],
                r["lat"],
                r["lon"],
                r["status"],
                r["calls"],
            )
            for r in rows
        ],
    )
    con.execute(f"""
        COPY (SELECT name, station_id, status = 'operational' AS operational FROM r ORDER BY name)
        TO '{name_map_path().as_posix()}' (FORMAT PARQUET)""")
    # One row per station: its most-called name's point and display name.
    con.execute("CREATE TABLE f (code VARCHAR, country VARCHAR)")
    con.executemany("INSERT INTO f VALUES (?, ?)", list(FOREIGN_STATIONS.items()))
    con.execute(f"""
        COPY (
            SELECT station_id AS bpuic, arg_max(display, calls) AS name,
                   any_value(code) AS code,
                   arg_max(lon, calls) AS lon, arg_max(lat, calls) AS lat,
                   coalesce(any_value(f.country), 'IT') AS country, true AS passenger,
                   DATE '1900-01-01' AS valid_from, DATE '9999-12-31' AS valid_to
            FROM r LEFT JOIN f USING (code)
            WHERE station_id IS NOT NULL
            GROUP BY station_id ORDER BY station_id
        ) TO '{CFG.dim_station_parquet.as_posix()}' (FORMAT PARQUET)""")
    summary = dict(
        con.execute("SELECT status, sum(calls) FROM r GROUP BY 1 ORDER BY 2 DESC").fetchall()
    )
    stations = con.execute("SELECT count(DISTINCT station_id) FROM r").fetchone()[0]
    return {"stations": stations, "calls_by_status": summary}


def limit_memory(con) -> None:
    """Three years of stops do not fit in this machine's memory: spill to disk instead."""
    tmp = CFG.root / "duckdb_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute("SET enable_progress_bar = false")
    con.execute("SET memory_limit = '2GB'")
    con.execute(f"SET temp_directory = '{tmp.as_posix()}'")
    con.execute("SET preserve_insertion_order = false")


def name_title(name: str) -> str:
    return " ".join(w.capitalize() for w in normalize(name).split(" "))


def name_map_path() -> Path:
    return CFG.root / "station_names.parquet"


# --- staging ------------------------------------------------------------------------------


def _files(days: list[date], kind: str) -> str:
    paths = [extract_dir() / f"{d}.{kind}.parquet" for d in days]
    missing = [p.name for p in paths if not p.exists()]
    if missing:
        raise RuntimeError(f"extracts missing: {missing[:5]}")
    return "[" + ", ".join(f"'{p.as_posix()}'" for p in paths) + "]"


def fn_zero_stations(con, days: list[date]) -> list[str]:
    """Stop names whose delays before FN_FIX_DAY are (almost) all exactly zero: the
    FerrovieNord stations TrainStats had not yet learned to read. Measured over every
    extracted day before the fix."""
    if not days:
        return []
    return [
        r[0]
        for r in con.execute(f"""
        WITH v AS (
            SELECT name, unnest([ra, rp]) AS d FROM read_parquet({_files(days, "stops")})
        )
        SELECT name FROM v WHERE regexp_full_match(d, '-?[0-9]+')
        GROUP BY name
        HAVING count(*) >= 100 AND count(*) FILTER (d = '0') >= {FN_ZERO_SHARE} * count(*)
        ORDER BY name""").fetchall()
    ]


def create_tables(con) -> None:
    from berg_pipeline.europe import stops

    stops.create_tables(con)
    # Everything the wire drops about a train, kept locally for journey metadata and the
    # cross-border builder: source id, number changes, route changes, foreign endpoints.
    con.execute("""
        CREATE TABLE IF NOT EXISTS it_trains (
            service_day DATE NOT NULL, trip_id VARCHAR NOT NULL, source_id VARCHAR,
            number VARCHAR, category VARCHAR, raw_c VARCHAR, raw_sub VARCHAR,
            origin VARCHAR, destination VARCHAR, planned_origin VARCHAR,
            planned_destination VARCHAR, route_note VARCHAR, number_changes VARCHAR,
            foreign_origin VARCHAR, foreign_destination VARCHAR,
            foreign_dep VARCHAR, foreign_arr VARCHAR,
            status VARCHAR NOT NULL  -- ran | part_cancelled | cancelled | no_stops
        )""")


def stage_days(con, days: list[date], fn_zero: list[str]) -> dict:
    """Extracts → stg_stops and it_trains for exactly these service days. Idempotent."""
    create_tables(con)
    first, last = min(days), max(days)
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _it_t AS SELECT * FROM read_parquet({_files(days, 'trains')})"
    )
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _it_s AS SELECT * FROM read_parquet({_files(days, 'stops')})"
    )
    if con.execute("SELECT count(*) FROM _it_t WHERE source_id IS NULL").fetchone()[0]:
        raise RuntimeError("trains without _id: a previous-database day, not stageable")
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _it_names AS SELECT * FROM '{name_map_path().as_posix()}'"
    )
    unknown = con.execute(
        "SELECT count(DISTINCT name) FROM _it_s ANTI JOIN _it_names USING (name)"
    ).fetchone()[0]
    if unknown:
        raise RuntimeError(f"{unknown} stop names not in the station map: rerun write_stations")
    con.execute("CREATE OR REPLACE TEMP TABLE _it_fn (name VARCHAR)")
    if fn_zero:
        con.executemany("INSERT INTO _it_fn VALUES (?)", [(n,) for n in fn_zero])

    # One row per train with its verdict. The same source id twice in a day is one train
    # recorded twice: the copy with more stops is kept. Two trains with one number where the
    # cancelled one carries the other's exact origin times is ViaggiaTreno's duplicate-number
    # bug (readme item 2): the cancelled copy is not a cancellation of its own.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _it_trains AS
        WITH t AS (
            SELECT CAST(service_day AS DATE) AS service_day, CAST(tidx AS INTEGER) AS tidx,
                   source_id, number, c, sub, p, a, rp, ra,
                   CAST(op AS BIGINT) AS op, CAST(oa AS BIGINT) AS oa,
                   oo, od, dl, cn, sep, sea, ope, oae, CAST(n_stops AS INTEGER) AS n_stops,
                   CASE WHEN coalesce(c, '') IN ('', 'ES*') AND sub IS NOT NULL THEN sub
                        WHEN coalesce(c, '') = '' THEN 'NC' ELSE c END AS category,
                   rp = 'X' OR ra = 'X' AS cancelled
            FROM _it_t
        ),
        dedup AS (
            SELECT *, row_number() OVER (PARTITION BY service_day, source_id
                                         ORDER BY n_stops DESC, tidx) AS k_id
            FROM t
        ),
        bug AS (
            SELECT DISTINCT x.service_day, x.tidx
            FROM dedup x JOIN dedup y
              ON x.service_day = y.service_day AND x.number = y.number AND x.tidx <> y.tidx
             AND x.cancelled AND NOT y.cancelled AND x.op = y.op AND x.oa = y.oa
            WHERE x.k_id = 1 AND y.k_id = 1
        )
        SELECT d.*, b.tidx IS NOT NULL AS dup_number_bug
        FROM dedup d LEFT JOIN bug b USING (service_day, tidx)
        WHERE d.k_id = 1""")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _it_stops AS
        WITH s AS (
            SELECT CAST(s.service_day AS DATE) AS service_day, CAST(s.tidx AS INTEGER) AS tidx,
                   CAST(s.seq AS INTEGER) AS seq, s.name,
                   nullif(CAST(s.s_arr AS BIGINT), 0) AS s_arr,
                   nullif(CAST(s.s_dep AS BIGINT), 0) AS s_dep,
                   s.ra, s.rp,
                   TRY_CAST(s.ra AS INTEGER) AS d_arr, TRY_CAST(s.rp AS INTEGER) AS d_dep,
                   s.name IN (SELECT name FROM _it_fn)
                     AND CAST(s.service_day AS DATE) < DATE '{FN_FIX_DAY}' AS fn_blind
            FROM _it_s s
            WHERE s.name NOT IN (SELECT name FROM _it_names WHERE operational)
        )
        SELECT *,
               -- a side exists unless the source says it does not (N) or was cancelled (S)
               s_arr IS NOT NULL AND ra NOT IN ('N', 'S') AS arr_live,
               s_dep IS NOT NULL AND rp NOT IN ('N', 'S') AS dep_live,
               coalesce(d_arr BETWEEN {DELAY_MIN_MIN} AND {DELAY_MAX_MIN} AND NOT fn_blind, false)
                   AS arr_ok,
               coalesce(d_dep BETWEEN {DELAY_MIN_MIN} AND {DELAY_MAX_MIN} AND NOT fn_blind, false)
                   AS dep_ok
        FROM s""")

    with ingest._transaction(con):
        con.execute("DELETE FROM source_days WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute("""
            INSERT INTO source_days
            SELECT service_day, count(*),
                   count(*) FILTER (cancelled AND NOT dup_number_bug)
            FROM _it_trains GROUP BY service_day""")
        con.execute("DELETE FROM stg_stops WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute("DELETE FROM it_trains WHERE service_day BETWEEN ? AND ?", [first, last])
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _it_labelled AS
            SELECT t.*,
                   t.category || ' ' || t.number AS label,
                   row_number() OVER (PARTITION BY t.service_day, t.category, t.number
                                      ORDER BY t.op, t.tidx) AS k,
                   EXISTS (SELECT 1 FROM _it_stops s
                           WHERE s.service_day = t.service_day AND s.tidx = t.tidx
                             AND (s.ra = 'S' OR s.rp = 'S')) AS part_cancelled
            FROM _it_trains t""")
        con.execute("""
            INSERT INTO it_trains
            SELECT service_day,
                   CASE WHEN k = 1 THEN label ELSE label || ' #' || k END,
                   source_id, number, category, c, sub, p, a, oo, od, dl, cn, sep, sea, ope, oae,
                   CASE WHEN cancelled THEN 'cancelled' WHEN n_stops = 0 THEN 'no_stops'
                        WHEN part_cancelled THEN 'part_cancelled' ELSE 'ran' END
            FROM _it_labelled WHERE NOT dup_number_bug""")
        con.execute("""
            INSERT INTO stg_stops
            SELECT s.service_day,
                   CASE WHEN t.k = 1 THEN t.label ELSE t.label || ' #' || t.k END AS trip_id,
                   NULL                                           AS operator,
                   t.number                                       AS train_number,
                   t.category,
                   -- A number change is searchable: "REG 2377/2378".
                   t.label || coalesce('/' || (SELECT string_agg(split_part(x, ',', 1), '/')
                                               FROM unnest(string_split(t.cn, ';')) u(x)), '')
                                                                  AS line,
                   coalesce(m.station_id, -CAST(hash(s.name) % 1000000000 AS BIGINT) - 1)
                                                                  AS station_id,
                   s.seq                                          AS stop_seq,
                   CASE WHEN s.arr_live THEN s.s_arr END           AS sched_arr,
                   CASE WHEN s.dep_live THEN s.s_dep END           AS sched_dep,
                   CASE WHEN s.arr_live THEN s.s_arr + 60 * CASE WHEN s.arr_ok THEN s.d_arr ELSE 0 END END,
                   CASE WHEN s.dep_live THEN s.s_dep + 60 * CASE WHEN s.dep_ok THEN s.d_dep ELSE 0 END END,
                   s.arr_live AND s.arr_ok                         AS arr_measured,
                   s.dep_live AND s.dep_ok                         AS dep_measured,
                   NULL                                           AS source_revision
            FROM _it_stops s
            JOIN _it_labelled t USING (service_day, tidx)
            LEFT JOIN _it_names m ON m.name = s.name
            WHERE NOT t.cancelled AND NOT t.dup_number_bug""")

        # Whole-minute delays: a reported arrival at or before the reported departure from
        # the previous stop cannot both be right. The arrival is not trusted and the hop
        # falls back to the timetable, which is positive for all but a handful of them.
        inconsistent = con.execute(f"""
            WITH h AS (
                SELECT service_day, trip_id, stop_seq, arr_measured, act_arr,
                       lag(act_dep) OVER w AS prev_dep, lag(dep_measured) OVER w AS prev_m
                FROM stg_stops WHERE service_day BETWEEN DATE '{first}' AND DATE '{last}'
                WINDOW w AS (PARTITION BY service_day, trip_id ORDER BY stop_seq)
            )
            UPDATE stg_stops s SET arr_measured = false
            FROM h
            WHERE s.service_day = h.service_day AND s.trip_id = h.trip_id
              AND s.stop_seq = h.stop_seq
              AND h.arr_measured AND h.prev_m AND h.act_arr <= h.prev_dep""").fetchone()[0]

    st = con.execute("""
        SELECT count(*), count(*) FILTER (cancelled), count(*) FILTER (dup_number_bug),
               count(*) FILTER (n_stops = 0 AND NOT cancelled),
               count(*) FILTER (dl IS NOT NULL OR oo IS NOT NULL OR od IS NOT NULL),
               count(*) FILTER (cn IS NOT NULL)
        FROM _it_trains""").fetchone()
    ss = con.execute(
        """
        SELECT count(*), count(*) FILTER (ra = 'S' OR rp = 'S'),
               count(*) FILTER (fn_blind AND (d_arr IS NOT NULL OR d_dep IS NOT NULL)),
               count(*) FILTER (d_arr NOT BETWEEN ? AND ? OR d_dep NOT BETWEEN ? AND ?)
        FROM _it_stops""",
        [DELAY_MIN_MIN, DELAY_MAX_MIN] * 2,
    ).fetchone()
    n_staged, n_unmatched, n_measured = con.execute(
        "SELECT count(*), count(*) FILTER (station_id < 0), "
        "count(*) FILTER (arr_measured OR dep_measured) FROM stg_stops "
        "WHERE service_day BETWEEN ? AND ?",
        [first, last],
    ).fetchone()
    dup_ids = con.execute(
        "SELECT count(*) - count(DISTINCT (service_day, source_id)) FROM _it_t"
    ).fetchone()[0]
    return {
        "trains": st[0],
        "trains_cancelled": st[1],
        "trains_dup_number_bug": st[2],
        "trains_without_stops": st[3],
        "trains_rerouted": st[4],
        "trains_number_change": st[5],
        "duplicate_source_ids": dup_ids,
        "calls": ss[0],
        "calls_cancelled": ss[1],
        "calls_fn_zero_blind": ss[2],
        "calls_extreme_delay": ss[3],
        "stops_staged": n_staged,
        "stops_measured": n_measured,
        "stops_unknown_station": n_unmatched,
        "arrivals_inconsistent": inconsistent,
    }
