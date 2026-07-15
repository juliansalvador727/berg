"""M0 spike: one day of Ist-Daten + GTFS stops → a binary the browser can lerp.

Deliberately not the real pipeline. Straight-line geometry (no OSM graph yet), coordinates
inlined per leg (no routes.bin yet), one day only. The point is to prove the whole loop end to
end — download, parse, leg-build, encode, render — before building any of it properly.

Usage:
    uv run python scripts/m0_export.py \
        --raw ../data/raw/2026-06-03_IstDaten.csv \
        --stops ../data/raw/stops-2026.txt \
        --out ../web/public/m0
"""

import argparse
import json
import struct
from array import array
from pathlib import Path

import duckdb

# Timestamps come in two shapes in the same file: 'DD.MM.YYYY HH:MM' (scheduled) and
# 'DD.MM.YYYY HH:MM:SS' (actual). try_strptime with a format list handles both.
TS_FORMATS = "['%d.%m.%Y %H:%M:%S','%d.%m.%Y %H:%M']"

# Switzerland, with enough margin to keep just-across-the-border stops that trains actually serve.
CH_BBOX = (5.9, 45.8, 10.5, 47.9)

MAX_LEG_DURATION_S = 3600

MAGIC = b"BERG"
FORMAT_VERSION = 1


def build(raw: Path, stops: Path, out: Path) -> None:
    c = duckdb.connect()
    c.execute("SET memory_limit='6GB'")

    # Station dimension. The BPUIC is the numeric prefix of the GTFS stop_id: both
    # '8509404:0:1' (platform) and 'Parent8509404' carry 8509404. Many stations exist ONLY as
    # platform rows, so extracting the prefix instead of matching plain 7-digit ids takes the
    # join from 47% to 90%.
    c.sql(
        f"CREATE VIEW stops_raw AS SELECT * FROM read_csv('{stops}', header=true, all_varchar=true)"
    )
    c.sql("""CREATE TABLE dim_station AS
        SELECT CAST(regexp_extract(stop_id, '^(?:Parent)?([0-9]+)', 1) AS BIGINT) AS bpuic,
               any_value(stop_name) AS stop_name,
               avg(CAST(stop_lat AS DOUBLE)) AS lat,
               avg(CAST(stop_lon AS DOUBLE)) AS lon
        FROM stops_raw
        WHERE regexp_matches(stop_id, '^(?:Parent)?[0-9]+')
        GROUP BY 1""")

    # all_varchar: never let read_csv autodetect types. It silently typed BETRIEBSTAG as DATE
    # on this very file, which is exactly the drift that breaks a 10-year backfill.
    c.sql(
        f"CREATE VIEW ist AS SELECT * FROM read_csv('{raw}', delim=';', header=true, all_varchar=true)"
    )

    # v2 measured status is 'REAL'. 'GESCHAETZT' (the v1 convention) never appears for trains
    # in v2 at all — verified against a full day. upper() on PRODUKT_ID because the feed
    # contains both 'Bus' and 'BUS'.
    c.sql(f"""CREATE VIEW ev AS
        SELECT FAHRT_BEZEICHNER AS trip,
               CAST(BPUIC AS BIGINT) AS bpuic,
               VERKEHRSMITTEL_TEXT AS cat,
               strptime(BETRIEBSTAG, '%d.%m.%Y') AS service_day,
               try_strptime(ABFAHRTSZEIT, {TS_FORMATS}) AS sched_dep,
               coalesce(try_strptime(AB_PROGNOSE, {TS_FORMATS}),
                        try_strptime(ABFAHRTSZEIT, {TS_FORMATS})) AS t_dep,
               coalesce(try_strptime(AN_PROGNOSE, {TS_FORMATS}),
                        try_strptime(ANKUNFTSZEIT, {TS_FORMATS})) AS t_arr,
               (AB_PROGNOSE_STATUS = 'REAL') AS dep_measured
        FROM ist
        WHERE upper(PRODUKT_ID) = 'ZUG' AND FAELLT_AUS_TF = 'false'""")

    # Order by SCHEDULED time. v2 has no stop-sequence column, and actual times go
    # non-monotonic when a train is delayed, which would scramble the stop order.
    c.sql("""CREATE VIEW legs AS
        SELECT trip, cat, t_dep, sched_dep, dep_measured,
               bpuic AS from_bpuic,
               lead(bpuic) OVER w AS to_bpuic,
               lead(t_arr) OVER w AS t_arr_next
        FROM ev WINDOW w AS (PARTITION BY trip, service_day ORDER BY sched_dep)""")

    c.sql(f"""CREATE TABLE m0 AS
        SELECT l.cat,
               CAST(epoch(l.t_dep) AS BIGINT) AS t_dep,
               CAST(date_diff('second', l.t_dep, l.t_arr_next) AS BIGINT) AS dur,
               f.lon AS from_lon, f.lat AS from_lat,
               t.lon AS to_lon,   t.lat AS to_lat
        FROM legs l
        JOIN dim_station f ON f.bpuic = l.from_bpuic
        JOIN dim_station t ON t.bpuic = l.to_bpuic
        WHERE l.to_bpuic IS NOT NULL AND l.t_dep IS NOT NULL AND l.t_arr_next IS NOT NULL
          AND date_diff('second', l.t_dep, l.t_arr_next) BETWEEN 1 AND {MAX_LEG_DURATION_S}
          AND f.lon BETWEEN {CH_BBOX[0]} AND {CH_BBOX[2]} AND f.lat BETWEEN {CH_BBOX[1]} AND {CH_BBOX[3]}
          AND t.lon BETWEEN {CH_BBOX[0]} AND {CH_BBOX[2]} AND t.lat BETWEEN {CH_BBOX[1]} AND {CH_BBOX[3]}
        ORDER BY t_dep""")

    cats = [
        r[0]
        for r in c.sql("SELECT DISTINCT cat FROM m0 WHERE cat IS NOT NULL ORDER BY 1").fetchall()
    ]
    cat_index = {name: i for i, name in enumerate(cats)}

    rows = c.sql("SELECT t_dep, dur, from_lon, from_lat, to_lon, to_lat, cat FROM m0").fetchall()
    n = len(rows)

    t_dep = array("I", (r[0] for r in rows))
    dur = array("H", (r[1] for r in rows))
    coords = array("f")
    for r in rows:
        coords.extend((r[2], r[3], r[4], r[5]))
    types = array("B", (cat_index.get(r[6], 255) for r in rows))

    out.mkdir(parents=True, exist_ok=True)

    # Struct-of-arrays so the browser can map each field to a typed array with zero parsing.
    blob = out / "legs.bin"
    with open(blob, "wb") as fh:
        fh.write(MAGIC)
        fh.write(struct.pack("<II", FORMAT_VERSION, n))
        for a in (t_dep, dur, coords, types):
            fh.write(a.tobytes())

    stations = c.sql(f"""SELECT DISTINCT d.bpuic, d.stop_name, d.lon, d.lat
        FROM dim_station d
        WHERE d.bpuic IN (SELECT from_bpuic FROM legs UNION SELECT to_bpuic FROM legs)
          AND d.lon BETWEEN {CH_BBOX[0]} AND {CH_BBOX[2]}
          AND d.lat BETWEEN {CH_BBOX[1]} AND {CH_BBOX[3]}""").fetchall()

    (out / "stations.json").write_text(
        json.dumps(
            [
                {"id": s[0], "name": s[1], "lon": round(s[2], 5), "lat": round(s[3], 5)}
                for s in stations
            ]
        )
    )

    t_min, t_max = c.sql("SELECT min(t_dep), max(t_dep + dur) FROM m0").fetchone()
    (out / "meta.json").write_text(
        json.dumps(
            {
                "legs": n,
                "types": cats,
                "t_min": t_min,
                "t_max": t_max,
                "max_leg_duration": MAX_LEG_DURATION_S,
                "service_day": raw.name[:10],
            }
        )
    )

    print(f"legs:     {n:,}")
    print(f"stations: {len(stations):,}")
    print(f"types:    {cats}")
    print(
        f"legs.bin: {blob.stat().st_size / 1e6:.2f} MB ({blob.stat().st_size / n:.1f} B/leg incl. coords)"
    )
    print(f"window:   {t_min} .. {t_max}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--raw", type=Path, required=True)
    p.add_argument("--stops", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    build(a.raw, a.stops, a.out)
