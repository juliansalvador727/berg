# Germany (DB Timetables API): what the data actually does

Dataset `de`, published under `datasets/de/` beside the Swiss root. The source is Deutsche
Bahn's Timetables API (IRIS) as crawled by
[piebro/deutsche-bahn-data](https://github.com/piebro/deutsche-bahn-data) and published on
[Hugging Face](https://huggingface.co/datasets/piebro/deutsche-bahn-data). The licence is CC BY
4.0 (Deutsche Bahn). Station positions come from DB's StaDa API, which the same crawl stores
(also CC BY 4.0). Every claim below was measured on the monthly files fetched 2026-09-24 at
revision `2a161c0b9957cd58297f841c818bb8cbbdaf2798`. The builder is
`pipeline/scripts/build_de.py`. The adapter is `berg_pipeline/europe/de.py`.

## Why not Bahn-Vorhersage

`europe.md` names the Bahn-Vorhersage archive (September 2021 onward, ODbL) for Germany. Its
yearly tarballs are distributed only through Mobilithek, which requires a logged-in account
and a per-offer access request. The offer API answers "Data offer not found or access denied"
without one. That cannot be done unattended, so it is still open.

The piebro archive crawls the **same API** with the same kind of result, so the time semantics
are identical (`final_prediction`). The difference is depth: the piebro archive covers every
IRIS station only from **2025-11-02**. From 2024-07 to 2025-11-01 it covers only the ~100
biggest stations, which is not a network: consecutive big stations are not consecutive stops.
The dataset therefore covers 2025-11-03 → 2026-08-30 and sits outside the 2023-2025 common
window. When Mobilithek access exists, Bahn-Vorhersage can backfill 2023-2025 into the same
dataset id and station namespace (both key stations by EVA number).

## The source

- One parquet file per month at
  `monthly_processed_data/data-YYYY-MM.parquet`. The files for 2025-11 → 2026-08 are 583-663 MB
  each, 6.2 GB in total, and hold 14.5-16M rows. Hugging Face serves them at about 7-8 MB/s.
- **The repository rewrites history.** The maintainer reprocessed every month in 2026-05
  (`train_name` split), 2026-08 (`is_canceled` split) and 2026-09 (added stops and replacement
  trains). The builder therefore reads one pinned revision and checks each file against the
  repository's LFS SHA-256.
- There is **one row per stop**, not per event. Each row has the planned and changed times for
  both sides, as naive Berlin local timestamps in whole minutes. The origin has no arrival and
  the terminus no departure.
- **A change time is never null.** When no realtime message arrived it equals the planned time.
  "On time" and "never reported" therefore look the same. That is why the dataset is labelled
  `final_prediction` and never `observed`, and every side counts as measured.
- The crawl runs every six hours on GitHub Actions. It fetches each station's planned hour
  (`plan`) and the change feed (`fchg`), which reaches back only about six hours. **A skipped
  run loses the stops planned in those hours outright.** See the gap section below.

## Rides, stops and order

- The IRIS stop id is `<trip hash>-<yymmddHHMM>-<position>`, and 49% of hashes are negative.
  All 15.6M ids of 2025-12 parse with that pattern.
  - The **hash repeats every day** for the same timetable trip, so `train_line_ride_id` alone is
    not a ride. It explains why 2025-12 shows only 105,916 distinct "ride ids" for ~45k rides a
    day.
  - The middle field is the ride's first scheduled departure. Its date is the service day.
  - The last field is the stop's position on the ride and always equals
    `train_line_station_num`. **Stop order is this position**, never time.
- **Positions skip stops IRIS does not list.** Foreign stops (Amsterdam, Basel SBB) and stops
  lost to a crawl gap leave a hole in the numbering. In 2025-12, 1.27M positions directly follow
  their predecessor and 71k skip ahead. A leg is built only between adjacent positions, so
  nothing bridges a hole. The same rule stops a leg from bridging a cancelled stop.
- **Added stops (`is_additional_stop`) are appended after the regular route.** They average
  position 97 against 10 for regular stops, and carry garbage planned times. In one example the
  added stop's planned departure was the ride's origin time. There are about 18-19k a month, and
  they are dropped and counted.
- Train numbers are **not unique per day**. S-Bahn networks reuse them, so S 42183 runs in
  Berlin and in Hamburg. On 2025-12-09, 999 labels were shared by 2-3 rides, and none of them
  were duplicate records (no shared station and time). The second ride with a label gets
  `" #2"`. The frontend's train-number parser drops that suffix.

## Categories

The IRIS category is a product (`ICE`, `RE`, `S`) for DB and many operators, but an operator
code for others (`ag` agilis, `erx`, `VIA`, `HLB`, `NWB`: about 130 codes in all).

- Products are kept as they are.
- An operator code takes its product from the line number's prefix: `RB23` → RB, `S5` → S, and
  `HBX`/`REX` → RE. Any other line under an operator code is RB, and without a line the code is
  kept (it shows as "Other").
- The raw category is kept as the stop's operator, and the line number as its line.
- **Road replacement is dropped**. That covers the train types `Bus`, `SEV`, `Taxi` and their
  spellings, and an `SEV`/`EV` line under a rail operator's code (`VIA` runs `SEV` lines). It is
  about 10% of rides but 2% of stops.
- Service groups in the UI:
  - S-Bahn: S.
  - Regional: RB, RE, IRE, MEX, RS, FEX, and the Austrian/Czech R and OS that reach German
    stations.
  - IC: IC, IR, D, FLX (Flixtrain) and WB (Westbahn).
  - Fast: ICE, ECE, EC, RJ, RJX, TGV, EST.
  - Night: NJ, EN, ES.

## Stations

- IRIS stations are keyed by EVA number, a zero-padded string in the source (`08002553`). The
  local station id is the integer (`station_namespace: de-eva`). Never use it as a global key:
  8000001-8099999 is DB's own range, but EVA numbers of other countries overlap other
  namespaces.
- **The crawl stores StaDa's seven category responses** in its raw files, with coordinates for
  every EVA number of a station (Berlin Gesundbrunnen has two). One snapshot per month is kept
  as `raw/stada-YYYY-MM.json`, and the latest position wins. The 2026-08 snapshot alone misses
  6 of the 5,374 EVAs in 2025-12: the Erzgebirge line Stollberg–Oelsnitz and Harra Nord. The
  2025-11 snapshot still lists them, so the union of all snapshots matches every stop (5,465
  stations). The one exception is two car-train (AZS) EVAs from 2026-08-24; see below.
- StaDa lists only German stations, so every station is `DE`. Legs with a foreign end cannot
  occur, because IRIS does not list foreign stops at all.

## Crawl gaps

Missing crawl runs appear as hours with almost no planned stops. The builder compares each UTC
hour's planned stops with the median for the same local hour of the week. It flags an hour
under 25% of that median. Holiday mornings (Christmas, Easter Monday) bottom out at about 35%
and are not flagged. Real holes sit under 5%.

- 203 UTC hours are flagged in the window. The largest holes are 2026-04-08 local 00-11 and six
  hours every night in most of July 2026. Isolated midnight hours through April-June 2026, three
  evening blocks (2025-11-23, 2026-02-02, 2026-03-16) and 2026-08-27/28 early mornings make up
  the rest.
- This matches the maintainer's own list (2025-11: 6 h, 2026-02: 7, 03: 10, 04: 34, 05: 31,
  06: 6, 07: 102).
- The hours are published as `source_gap_hours` in the manifest and `quality.json`. The UI shows
  "No Germany data for HH:00-HH:59 UTC · source gap" instead of an empty map. Days with holes
  are still published: a twelve-hour hole leaves more than half a day, far above the 50,000-leg
  floor.

## Timezones and window

- Local times are converted from Europe/Berlin. The window contains the 2026-03-29 spring
  change and no autumn change, so there are no ambiguous hours.
- The monthly files are cut by local time. The first UTC day that is complete in both
  directions is 2025-11-03. The last is 2026-08-30: UTC 2026-08-31 needs the local hours
  00:00-02:00 of 2026-09-01, which are in the unpublished September file.

## Measured volume

Full build of 2025-11-03 → 2026-08-30 (`data/datasets/de/quality.json`):

| Item | Value |
|---|---:|
| UTC days | 301, 0 missing |
| Published legs | 124,204,786 (248k-478k per day) |
| Journeys | 10,294,977 (about 36.7k on a weekday, under the uint16 limit) |
| Leg + journey bytes | 652.5 MB (5.25 bytes per leg with the sidecar, zstd level 19) |
| Routes / fallbacks | 19,586 / 67 |
| Station pairs snapped | 5,399 of 5,409 stations, p50 8.8 m |
| Source gap hours | 203 |

- Per month, about 13-14.7M stops are staged. 74-145k rides are road replacement and dropped,
  as are 15-35k added stops. 0-4 rides are malformed (planned times run backwards).
- Quarantine over the window:
  - `zero_duration`: 648,349 (0.5%, hops planned to the same minute).
  - `negative_duration`: 68,748.
  - `absurd_duration`: 14.
  - `unmatched_station`: 391. These are two EVAs (8085311, 8030918) that first appear on
    2026-08-24 with no station name, served only by the car train `AZS`. No StaDa snapshot lists
    them. They are not passenger stations and stay unpublished.
- The build stages a month in about 2.5 minutes, and the whole window takes 21 minutes. It
  records each finished month in `stage_progress.json` and resumes after the last one. On this
  8 GB WSL machine, running it beside the OSM extract got the VM OOM-killed, so DuckDB is capped
  at 2 GB and 4 threads.

## Geometry

No public Overpass instance answers the whole-country rail query (HTTP 504), and a tiled fetch
from `overpass.kumi.systems` kept failing on the Munich tile. The geometry comes from the
Geofabrik extract instead. It is 4.8 GB, and 8 parallel range requests to the GWDG mirror took
about 25 minutes. Its MD5 was checked against the mirror's `.md5` file.

`python -m berg_geometry.merge_osm --extract` cuts it to the rail network in 37 s at 2.5 GB
peak: 1.82M nodes and 244k ways, 20 MB. Loading the full extract with node locations directly
would not fit in RAM. Routing 19,586 pairs from 5,398 sources then takes 20 minutes at 1 GB.
