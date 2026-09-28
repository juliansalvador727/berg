"""Download Digitraffic's per-date train archive for a date range. Resumable: days already on
disk are trusted, because fetch_day only renames a validated body into place.

Usage:
    uv run python scripts/fetch_fi.py 2023-01-01 2025-12-31
"""

import sys
import time
from datetime import date, timedelta

import httpx

from berg_pipeline.europe import fi


def main(first: date, last: date) -> int:
    day, done, t0 = first, 0, time.monotonic()
    with httpx.Client(headers=fi.USER_AGENT_HEADERS, timeout=120) as client:
        while day <= last:
            existed = fi.raw_path(day).exists()
            fi.fetch_day(day, client=client)
            done += 1
            if not existed:
                time.sleep(0.5)  # a polite client: one request at a time, never a burst
            if done % 30 == 0:
                print(f"{day}: {done} days ({time.monotonic() - t0:.0f}s)", flush=True)
            day += timedelta(days=1)
    print(f"done: {done} days in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main(date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2])))
