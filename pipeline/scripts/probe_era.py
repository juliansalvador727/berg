"""M1 step 1: prove the v1 era parses, before building anything on top of it.

83 of 102 usable months are v1 and M0 never touched one. This pulls a single day out of a
remote monthly ZIP and answers the questions that decide the era-view design:

  - does the v1 header match v2's, and which columns moved?
  - what is v1's measured-status enum really? (plan.md expects GESCHAETZT; v2 says REAL)
  - do the two pre-2021 naming eras parse at all?
  - do the same traps apply — PRODUKT_ID case drift, dual timestamp formats?

Usage:
    uv run python scripts/probe_era.py \
        --url https://archive.opentransportdata.swiss/istdaten/2018/18_01.zip \
        --cache ../data/raw
"""

import argparse
import shutil
from pathlib import Path

import duckdb

from berg_pipeline.remote_zip import open_remote_zip

TS_FORMATS = "['%d.%m.%Y %H:%M:%S','%d.%m.%Y %H:%M']"


def fetch_day(url: str, cache: Path, member: str | None) -> Path:
    """Pull one CSV member out of the remote ZIP, cached on disk."""
    with open_remote_zip(url) as zf:
        names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
        if not names:
            raise SystemExit(f"no CSV members in {url}\nmembers: {zf.namelist()[:10]}")
        print(f"members:  {len(names)} CSV ({names[0]} .. {names[-1]})")

        name = member or names[0]
        info = zf.getinfo(name)
        print(
            f"member:   {name}  {info.compress_size / 1e6:.0f} MB zipped "
            f"→ {info.file_size / 1e6:.0f} MB raw"
        )

        cache.mkdir(parents=True, exist_ok=True)
        dest = cache / Path(name).name
        if dest.exists() and dest.stat().st_size == info.file_size:
            print(f"cached:   {dest}")
            return dest
        with zf.open(name) as src, open(dest, "wb") as out:
            shutil.copyfileobj(src, out, length=1 << 20)
        return dest


def probe(csv: Path) -> None:
    c = duckdb.connect()
    c.execute("SET memory_limit='6GB'")
    # all_varchar for the same reason the real ingest uses it: autodetect silently typed
    # BETRIEBSTAG as DATE on the v2 file, and this probe exists to catch drift, not hide it.
    c.sql(
        f"CREATE VIEW ist AS SELECT * FROM read_csv('{csv}', delim=';', header=true, all_varchar=true)"
    )

    cols = [r[0] for r in c.sql("DESCRIBE ist").fetchall()]
    print(f"\ncolumns:  {len(cols)}")
    for col in cols:
        print(f"  {col}")

    print("\nPRODUKT_ID:")
    for v, n in c.sql("SELECT PRODUKT_ID, count(*) FROM ist GROUP BY 1 ORDER BY 2 DESC").fetchall():
        print(f"  {v!r:24} {n:>10,}")

    for col in ("AB_PROGNOSE_STATUS", "AN_PROGNOSE_STATUS"):
        if col not in cols:
            print(f"\n{col}: ABSENT")
            continue
        print(f"\n{col} for trains:")
        for v, n in c.sql(f"""SELECT {col}, count(*) FROM ist
                WHERE upper(PRODUKT_ID) = 'ZUG' GROUP BY 1 ORDER BY 2 DESC""").fetchall():
            print(f"  {v!r:24} {n:>10,}")

    print("\ntimestamp formats (trains, non-null):")
    for col in ("ABFAHRTSZEIT", "AB_PROGNOSE", "ANKUNFTSZEIT", "AN_PROGNOSE"):
        if col not in cols:
            continue
        row = c.sql(f"""SELECT count(*),
                   count(*) FILTER (WHERE regexp_matches({col}, '^\\d\\d\\.\\d\\d\\.\\d{{4}} \\d\\d:\\d\\d$')),
                   count(*) FILTER (WHERE regexp_matches({col}, '^\\d\\d\\.\\d\\d\\.\\d{{4}} \\d\\d:\\d\\d:\\d\\d$')),
                   count(*) FILTER (WHERE try_strptime({col}, {TS_FORMATS}) IS NULL)
            FROM ist WHERE upper(PRODUKT_ID) = 'ZUG' AND {col} IS NOT NULL AND {col} <> ''""").fetchone()
        print(
            f"  {col:14} n={row[0]:>8,}  HH:MM={row[1]:>8,}  HH:MM:SS={row[2]:>8,}  unparsed={row[3]:>6,}"
        )

    print("\nvolume (trains):")
    row = c.sql("""SELECT count(*), count(DISTINCT FAHRT_BEZEICHNER), count(DISTINCT BPUIC),
               count(*) FILTER (WHERE FAELLT_AUS_TF NOT IN ('false','0'))
        FROM ist WHERE upper(PRODUKT_ID) = 'ZUG'""").fetchone()
    print(
        f"  stop events {row[0]:,} · runs {row[1]:,} · stations {row[2]:,} · cancelled {row[3]:,}"
    )

    print("\nBETRIEBSTAG / FAELLT_AUS_TF sample:")
    for r in c.sql("""SELECT DISTINCT BETRIEBSTAG, FAELLT_AUS_TF, ZUSATZFAHRT_TF FROM ist
        LIMIT 5""").fetchall():
        print(f"  {r}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True)
    p.add_argument("--cache", type=Path, default=Path("../data/raw"))
    p.add_argument("--member", default=None, help="CSV member name; default = first day")
    a = p.parse_args()
    probe(fetch_day(a.url, a.cache, a.member))
