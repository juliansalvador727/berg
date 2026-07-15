"""Build dim_station (SCD Type 2) by diffing archived GTFS stops.txt snapshots.

A 2018 stop event must join to the 2018 name and coordinates, so the station dimension is
validity-ranged: one row per (bpuic, valid_from). The archive publishes GTFS ~weekly back to
2016, and a snapshot's attributes hold until the next snapshot changes them — so SCD2 here is
just a diff between consecutive stops.txt files.

Two rules earn their keep (see docs/data-notes.md):

  - **Coordinates come from the station-level row, not a platform centroid.** Averaging
    platforms is fine for one map, but across eras it manufactures changes: the platform
    centroid matches the station row for 86.5% of stations in 2023 and only 11.0% in 2026.
    Station rows agree with each other 100% of the time, so the preference chain never jumps.
  - **Round before comparing.** Coordinate precision drifts (13 decimals in 2018, 8 in 2026),
    so raw float equality would emit a spurious change for nearly every station.

CLI wrapper: scripts/build_dim_station.py. Dagster asset: assets/dimensions.py.
"""

import gzip
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

from berg_pipeline.gtfs import Snapshot, list_snapshots, latest_per_day, stops_member
from berg_pipeline.remote_zip import open_remote_zip

# ~1.1 m. Enough to see a station move, coarse enough to absorb the archive's precision drift.
COORD_DP = 5


def fetch(snap: Snapshot, cache: Path, attempts: int = 4) -> Path:
    dest = cache / f"stops-{snap.day.isoformat()}.txt.gz"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    # The host hiccups under sustained concurrency — roughly one failure per few hundred
    # fetches, surfacing as BadZipFile from a truncated range read. Retry the whole snapshot
    # rather than trust a partial one.
    for attempt in range(1, attempts + 1):
        try:
            with open_remote_zip(snap.url) as zf:
                with zf.open(stops_member(zf.namelist())) as fh:
                    raw = fh.read()
            break
        except Exception as e:  # noqa: BLE001 — any failure is worth one more try
            if attempt == attempts:
                raise RuntimeError(
                    f"{snap.day} failed after {attempts} attempts: {snap.url}"
                ) from e
            time.sleep(2**attempt)
    tmp = dest.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_bytes(gzip.compress(raw))
    tmp.rename(dest)  # atomic: a half-written cache entry is worse than a missing one
    return dest


# One snapshot in the archive is truncated (2018-08-15: 67 MB of stream, no central directory).
# Skipping it is harmless — snapshots are weekly, so a neighbour covers the period within days.
# Skipping MANY would not be: it would silently coarsen the dimension. Fail loudly past this.
MAX_BROKEN_FRACTION = 0.02


def fetch_all(snaps: list[Snapshot], cache: Path, workers: int) -> list[Snapshot]:
    """Fetch every snapshot's stops.txt. Returns the usable ones, loudly skipping the rest."""
    cache.mkdir(parents=True, exist_ok=True)

    def one(s: Snapshot) -> tuple[Snapshot, str | None]:
        try:
            fetch(s, cache)
            return s, None
        except Exception as e:  # noqa: BLE001 — reported and counted below, never swallowed
            return s, str(e)

    ok, broken, done = [], [], 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for snap, err in ex.map(one, snaps):
            done += 1
            if err is None:
                ok.append(snap)
            else:
                broken.append(snap)
                print(f"  SKIP {snap.day}: {err}", flush=True, file=sys.stderr)
            if done % 50 == 0 or done == len(snaps):
                print(f"  fetched {done}/{len(snaps)}", flush=True, file=sys.stderr)

    if broken:
        frac = len(broken) / len(snaps)
        print(
            f"\n{len(broken)}/{len(snaps)} snapshots unusable ({frac:.1%}): "
            f"{[s.day.isoformat() for s in broken]}",
            file=sys.stderr,
        )
        if frac > MAX_BROKEN_FRACTION:
            raise RuntimeError(
                f"{frac:.1%} of GTFS snapshots failed (> {MAX_BROKEN_FRACTION:.0%}) — "
                "that is an outage or a changed archive, not the known bad file. "
                "Refusing to build a silently coarser dim_station."
            )
    return ok


def build(cache: Path, out: Path) -> dict:
    c = duckdb.connect()
    c.execute("SET memory_limit='6GB'")

    # all_varchar for the same reason the ingest uses it: never let a schema change three years
    # in become a silent cast. Newer snapshots add columns (platform_code, original_stop_id),
    # so select by name and union_by_name across the glob.
    c.sql(f"""CREATE VIEW raw AS
        SELECT CAST(regexp_extract(filename, 'stops-(\\d{{4}}-\\d{{2}}-\\d{{2}})\\.txt\\.gz$', 1)
                    AS DATE) AS snap,
               stop_id, stop_name, stop_lat, stop_lon, location_type
        FROM read_csv('{cache}/stops-*.txt.gz', all_varchar=true, header=true,
                      filename=true, union_by_name=true)""")

    # The BPUIC is the numeric prefix of stop_id, across four id shapes:
    #   8501008 (bare) · Parent8501008 · 8501008:0:1 (platform) · 8004238P (2018 station row)
    # Preference chain for which row supplies the coordinate. No single kind exists in every
    # era: 2018 has no Parent rows at all, and 15,465 stations in 2026 have no bare row.
    c.sql(f"""CREATE TABLE tagged AS
        SELECT snap,
               CAST(regexp_extract(stop_id, '^(?:Parent)?([0-9]+)', 1) AS BIGINT) AS bpuic,
               stop_name,
               round(CAST(stop_lat AS DOUBLE), {COORD_DP}) AS lat,
               round(CAST(stop_lon AS DOUBLE), {COORD_DP}) AS lon,
               CASE WHEN regexp_matches(stop_id, '^[0-9]+$')      THEN 1  -- bare station row
                    WHEN starts_with(stop_id, 'Parent')           THEN 2  -- parent station row
                    WHEN location_type = '1'                      THEN 3  -- 2018's '8004238P'
                    ELSE 4                                                -- platform
               END AS kind
        FROM raw
        WHERE regexp_matches(stop_id, '^(?:Parent)?[0-9]+')
          AND try_cast(stop_lat AS DOUBLE) IS NOT NULL
          AND try_cast(stop_lon AS DOUBLE) IS NOT NULL""")

    # Keep only the best-available kind per station per snapshot. For kinds 1-3 that is a single
    # row; kind 4 means a platform-only station (Buchs SG in 2018), where the centroid is the
    # only thing on offer. min() not any_value(): a nondeterministic pick would show up as a
    # phantom attribute change on the next snapshot.
    c.sql("""CREATE TABLE obs AS
        WITH ranked AS (
            SELECT *, min(kind) OVER (PARTITION BY snap, bpuic) AS best FROM tagged
        )
        SELECT snap, bpuic,
               min(stop_name) AS name,
               round(avg(lat), 5) AS lat,
               round(avg(lon), 5) AS lon
        FROM ranked WHERE kind = best
        GROUP BY snap, bpuic""")

    # Snapshot sequence: SCD2 needs "the next snapshot", not "the next day".
    c.sql("""CREATE TABLE seq AS
        SELECT snap, row_number() OVER (ORDER BY snap) AS idx
        FROM (SELECT DISTINCT snap FROM obs)""")
    n_snaps = c.sql("SELECT count(*) FROM seq").fetchone()[0]

    # A new segment starts when an attribute changes, when the station first appears, or when
    # it was ABSENT from the previous snapshot (idx gap) — a station that closes and reopens
    # must not have its closure silently spanned by one long validity range.
    c.sql("""CREATE TABLE seg AS
        SELECT *, sum(is_new) OVER (PARTITION BY bpuic ORDER BY idx) AS seg FROM (
            SELECT o.bpuic, o.name, o.lat, o.lon, s.snap, s.idx,
                   CASE WHEN lag(s.idx)  OVER w IS NULL         THEN 1
                        WHEN lag(s.idx)  OVER w <> s.idx - 1    THEN 1
                        WHEN lag(o.name) OVER w IS DISTINCT FROM o.name THEN 1
                        WHEN lag(o.lat)  OVER w IS DISTINCT FROM o.lat  THEN 1
                        WHEN lag(o.lon)  OVER w IS DISTINCT FROM o.lon  THEN 1
                        ELSE 0 END AS is_new
            FROM obs o JOIN seq s USING (snap)
            WINDOW w AS (PARTITION BY o.bpuic ORDER BY s.idx)
        )""")

    # valid_to is the day before the snapshot that ended the segment; the open segment runs to
    # 9999-12-31. Ranges are therefore closed-closed and gapless within a station's lifetime.
    c.sql(f"""CREATE TABLE dim_station AS
        WITH g AS (
            SELECT bpuic, seg, min(name) AS name, min(lat) AS lat, min(lon) AS lon,
                   min(snap) AS valid_from, max(idx) AS last_idx
            FROM seg GROUP BY bpuic, seg
        )
        SELECT g.bpuic, g.name, g.lon, g.lat, g.valid_from,
               CASE WHEN g.last_idx = {n_snaps} THEN DATE '9999-12-31'
                    ELSE (SELECT s.snap - INTERVAL 1 DAY FROM seq s WHERE s.idx = g.last_idx + 1)
               END AS valid_to
        FROM g
        ORDER BY bpuic, valid_from""")

    out.parent.mkdir(parents=True, exist_ok=True)
    c.sql(f"COPY dim_station TO '{out}' (FORMAT PARQUET, COMPRESSION zstd)")

    n_rows, n_stations, first, last = c.sql(
        "SELECT count(*), count(DISTINCT bpuic), min(valid_from), max(valid_from) FROM dim_station"
    ).fetchone()
    print(f"\nsnapshots:    {n_snaps}")
    print(f"stations:     {n_stations:,}")
    print(f"dim rows:     {n_rows:,}  ({n_rows / n_stations:.2f} per station)")
    print(f"valid_from:   {first} .. {last}")
    print(f"parquet:      {out}  ({out.stat().st_size / 1e6:.1f} MB)")

    print("\nsegments per station:")
    for n, c_ in c.sql("""SELECT n, count(*) FROM (
            SELECT bpuic, count(*) AS n FROM dim_station GROUP BY bpuic
        ) GROUP BY n ORDER BY n LIMIT 8""").fetchall():
        print(f"  {n:>3} range(s): {c_:>7,} stations")

    print("\nmost-revised stations:")
    for b, n, nm in c.sql("""SELECT bpuic, count(*) n, max(name) FROM dim_station
        GROUP BY bpuic ORDER BY n DESC LIMIT 5""").fetchall():
        print(f"  {b}  {n:>3} ranges  {nm}")

    # A validity range that overlaps its neighbour would make the fact join emit duplicate legs.
    bad = c.sql("""SELECT count(*) FROM (
        SELECT bpuic, valid_to, lead(valid_from) OVER (PARTITION BY bpuic ORDER BY valid_from) nf
        FROM dim_station) WHERE nf IS NOT NULL AND nf <= valid_to""").fetchone()[0]
    print(f"\noverlapping ranges: {bad}  {'OK' if bad == 0 else '*** BROKEN ***'}")
    if bad:
        raise RuntimeError(
            f"{bad} overlapping validity ranges — the fact join would duplicate legs"
        )

    return {
        "snapshots": n_snaps,
        "stations": n_stations,
        "rows": n_rows,
        "valid_from_min": str(first),
        "valid_from_max": str(last),
        "bytes": out.stat().st_size,
    }


def build_full(cache: Path, out: Path, workers: int = 8, limit: int | None = None) -> dict:
    """List, fetch (cache-aware), and build — the whole dimension in one call."""
    snaps = latest_per_day(list_snapshots())
    if limit:
        snaps = snaps[-limit:]
    print(f"snapshots: {len(snaps)}  ({snaps[0].day} .. {snaps[-1].day})", file=sys.stderr)
    usable = fetch_all(snaps, cache, workers)
    stats = build(cache, out)
    stats["snapshots_usable"] = len(usable)
    stats["snapshots_listed"] = len(snaps)
    return stats
