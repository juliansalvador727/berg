"""Archive census for Italy: every fetched TrainStats day → extract + one census row.

    uv run python scripts/census_it.py            # all days in the ledger
    → data/datasets/it/census.csv, and a coverage summary on stdout

Extraction is the parse every later stage reads, so this is also the first build step. A day
whose file fails to parse stops the run with the error; a day the ledger marks broken at the
source is listed as missing.
"""

import csv
import json
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import date

from berg_pipeline.europe import build, it, it_fetch
from berg_pipeline.europe.config import ITALY as CFG

WORKERS = 4  # each parse holds one ~25 MB JSON day in memory several times over

COLS = (
    "day source compressed_bytes summary_schema platforms circulated trains trains_with_stops calls "
    "arr_numeric dep_numeric arr_nd dep_nd cancelled_trains cancelled_stops "
    "part_cancelled_trains rerouted_trains number_changes international duplicate_ids "
    "unique_stations sha256 categories"
).split()


def main() -> int:
    ledger = it_fetch.load_ledger(CFG)
    ok = sorted(date.fromisoformat(d) for d, r in ledger.items() if r["status"] == "ok")
    with ProcessPoolExecutor(WORKERS) as pool:
        rows = list(pool.map(it.extract_day, ok, chunksize=4))
    out = CFG.root / "census.csv"
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, COLS)
        w.writeheader()
        for r in rows:
            r = dict(r, sha256=ledger[r["day"]]["sha256"], categories=json.dumps(r["categories"]))
            w.writerow({k: r[k] for k in COLS})

    first, last = ok[0], ok[-1]
    have = set(ok)
    missing = [d for d in build.daterange(first, last) if d not in have]
    trains = sorted(r["trains"] for r in rows)
    median = trains[len(trains) // 2]
    thin = [(r["day"], r["trains"]) for r in rows if r["trains"] < 0.6 * median]
    print(f"days {len(ok)} from {first} to {last}; median trains/day {median}")
    print("missing:", [str(d) for d in missing])
    print("broken at source:", [d for d, r in ledger.items() if r["status"] != "ok"])
    print("thin (<60% of median trains):", thin)
    print(
        "calls/day median:",
        sorted(r["calls"] for r in rows)[len(rows) // 2],
        "first day with platforms:",
        next((r["day"] for r in rows if r["platforms"]), None),
        "first v2 summary:",
        next((r["day"] for r in rows if r["summary_schema"] == "v2"), None),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
