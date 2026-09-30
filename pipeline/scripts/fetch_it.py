"""Fetch TrainStats' daily archive (Dropbox current, Mega legacy) into the raw area.

    uv run python scripts/fetch_it.py                  # both sources, the whole window
    uv run python scripts/fetch_it.py --refresh        # re-download the Dropbox folder too

Resumable: a day already in days/ is never downloaded again. Mega's anonymous transfer quota
pauses the legacy download for a while every few GB; it resumes on its own.
"""

import argparse
import sys
from datetime import date

from berg_pipeline.europe import it_fetch
from berg_pipeline.europe.config import ITALY as CFG

# The first day TrainStats' current database exported (see docs/data-notes-it.md).
FIRST_CURRENT_FORMAT_DAY = date(2023, 2, 1)
LAST_MEGA_DAY = date(2024, 6, 7)


def main(refresh: bool, skip_mega: bool, skip_dropbox: bool) -> int:
    if not skip_dropbox:
        print("dropbox: new days", it_fetch.fetch_dropbox(CFG, refresh=refresh), flush=True)
    if not skip_mega:
        added = it_fetch.fetch_mega(CFG, FIRST_CURRENT_FORMAT_DAY, LAST_MEGA_DAY)
        print("mega: new days", added)
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--refresh", action="store_true", help="re-download the Dropbox folder")
    p.add_argument("--skip-mega", action="store_true")
    p.add_argument("--skip-dropbox", action="store_true")
    a = p.parse_args()
    sys.exit(main(a.refresh, a.skip_mega, a.skip_dropbox))
