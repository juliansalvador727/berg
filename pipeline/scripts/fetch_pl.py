"""Download PKP Intercity train runs, with per-stop times and delays, from spoznienia.me's public
API (https://api.spoznienia.me, github.com/marekk13/PKP-Intercity-Train-Delay-Scraper).

The API is one person's free server, limited to 60 requests a minute per IP, so this client
stays well under that: one request at a time, one every REQUEST_INTERVAL_S, and it backs off on
429 and 5xx. Resumable: a finished day is one JSON file renamed into place; a day in progress
keeps its fetched runs in a .partial.jsonl beside it.

Usage:
    uv run python scripts/fetch_pl.py 2025-10-23 2026-08-30
"""

import json
import sys
import time
from datetime import date, timedelta

import httpx

from berg_pipeline import paths

API = "https://api.spoznienia.me"
RAW = paths.DATA_ROOT / "datasets" / "pl" / "raw" / "api"
HEADERS = {
    "User-Agent": "berg/europe-archive (historical train map; one request at a time)",
    "Accept-Encoding": "gzip",
}
REQUEST_INTERVAL_S = 1.5  # 40 a minute against a limit of 60
PAGE = 500

_last_request = 0.0


def get(client: httpx.Client, path: str, params: dict | None = None):
    global _last_request
    for attempt in range(12):
        wait = _last_request + REQUEST_INTERVAL_S - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request = time.monotonic()
        try:
            r = client.get(API + path, params=params)
        except httpx.TransportError as e:
            delay = min(600, 15 * 2**attempt)  # Render cold starts take up to a minute
            print(f"  {path}: {type(e).__name__}, retry in {delay}s", flush=True)
            time.sleep(delay)
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429 or r.status_code >= 500:
            delay = int(r.headers.get("Retry-After", 0)) or min(600, 30 * 2**attempt)
            print(f"  {path}: HTTP {r.status_code}, retry in {delay}s", flush=True)
            time.sleep(delay)
            continue
        r.raise_for_status()
    raise RuntimeError(f"{path}: giving up after repeated failures")


def fetch_day(client: httpx.Client, day: date) -> tuple[int, int]:
    out = RAW / f"{day.isoformat()}.json"
    if out.exists():
        return -1, -1
    runs, offset = [], 0
    while True:
        page = get(
            client, "/train-runs", {"date": day.isoformat(), "offset": offset, "limit": PAGE}
        )
        runs += page
        if len(page) < PAGE:
            break
        offset += PAGE
    partial = out.with_suffix(".partial.jsonl")
    details = {}
    if partial.exists():
        for line in partial.read_text().splitlines():
            d = json.loads(line)
            details[d["id"]] = d
    with partial.open("a") as f:
        for run in runs:
            # A run without a scheduled departure has no stops in the API (checked on the first
            # and latest days), so asking for its detail would only cost the server a request.
            if run["id"] in details or not run.get("scheduled_departure"):
                continue
            d = get(client, f"/train-runs/{run['id']}")
            details[d["id"]] = d
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
            f.flush()
    body = {
        "date": day.isoformat(),
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runs": [details.get(r["id"], r) for r in runs],
    }
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(body, ensure_ascii=False))
    tmp.rename(out)
    partial.unlink()
    return len(runs), len(details)


def main(first: date, last: date) -> int:
    RAW.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    with httpx.Client(headers=HEADERS, timeout=120) as client:
        day = first
        while day <= last:
            n, with_stops = fetch_day(client, day)
            if n >= 0:
                print(
                    f"{day}: {n} runs, {with_stops} with stops ({time.monotonic() - t0:.0f}s)",
                    flush=True,
                )
            day += timedelta(days=1)
    print(f"done in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main(date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2])))
