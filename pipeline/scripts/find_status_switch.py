"""Find the month where AB_PROGNOSE_STATUS flips from GESCHAETZT to REAL.

The v1→v2 file-format boundary (2025-07, when SLOID appears) is NOT the same boundary as the
measured-status enum: 2023-06 is still 21-column v1 but already reports REAL. Ingest has to
pick the measured value by *date*, so the pipeline needs the real switchover month.

Cheap by design: streams only the head of one day per month and counts enum tokens, rather
than materializing 130 MB days.

Usage:
    uv run python scripts/find_status_switch.py 2018 2019 2020 2021 2022 2023
"""

import argparse
import io
import sys

from berg_pipeline.archive import url_for_month
from berg_pipeline.remote_zip import open_remote_zip

# Enough rows to see thousands of train stop events; trains are a minority of the feed.
SCAN_BYTES = 24 << 20


def scan_month(year: int, month: int, day: str | None = None) -> dict:
    url = url_for_month(year, month)
    with open_remote_zip(url) as zf:
        names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
        if not names:
            return {"error": "no CSV members"}
        # Member paths drift constantly (jan18/, 18_10/, 19_1/, bare, ist-daten-2023-04/) —
        # never construct one, always match against the listing.
        if day:
            hits = [n for n in names if day in n]
            if not hits:
                return {"error": f"no member for {day}"}
            name = hits[0]
        else:
            name = names[0]
        with zf.open(name) as fh:
            head = fh.read(SCAN_BYTES)

    text = io.TextIOWrapper(io.BytesIO(head), encoding="utf-8", errors="replace")
    header = text.readline().rstrip("\n").split(";")
    try:
        i_prod = header.index("PRODUKT_ID")
        i_stat = header.index("AB_PROGNOSE_STATUS")
    except ValueError as e:
        return {"error": f"missing column: {e}", "cols": len(header)}

    counts: dict[str, int] = {}
    lines = text.read().split("\n")[:-1]  # drop the truncated tail line
    for line in lines:
        f = line.split(";")
        if len(f) <= max(i_prod, i_stat):
            continue
        if f[i_prod].upper() != "ZUG":
            continue
        counts[f[i_stat]] = counts.get(f[i_stat], 0) + 1

    return {
        "member": name,
        "cols": len(header),
        "sloid": "SLOID" in header,
        "counts": counts,
    }


def verdict(counts: dict) -> str:
    real = counts.get("REAL", 0)
    gesch = counts.get("GESCHAETZT", 0)
    if real and gesch:
        return f"MIXED (REAL={real:,} GESCHAETZT={gesch:,})"
    if real:
        return "REAL"
    if gesch:
        return "GESCHAETZT"
    return "neither"


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("years", nargs="+", type=int)
    p.add_argument("--months", nargs="*", type=int, default=[1, 4, 7, 10])
    p.add_argument("--days", nargs="*", default=[None], help="YYYY-MM-DD; default = first member")
    a = p.parse_args()

    for year in a.years:
        for month in a.months:
            for day in a.days:
                label = day or f"{year}-{month:02d}"
                try:
                    r = scan_month(year, month, day)
                except Exception as e:  # noqa: BLE001 — a probe; report and keep going
                    print(f"{label}  ERROR {type(e).__name__}: {e}", flush=True)
                    continue
                if "error" in r:
                    print(f"{label}  {r['error']}", flush=True)
                    continue
                print(
                    f"{label}  cols={r['cols']:2d} sloid={str(r['sloid']):5}  "
                    f"{verdict(r['counts']):40}  {r['member']}",
                    flush=True,
                )
    sys.exit(0)
