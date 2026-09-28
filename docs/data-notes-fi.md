# Finland (Digitraffic): what the data actually does

Dataset `fi`, published under `datasets/fi/` beside the Swiss root. Source: Fintraffic's
Digitraffic railway API, licence CC BY 4.0, attribution "Source: Fintraffic / digitraffic.fi,
license CC 4.0 BY". Every claim below was measured against real responses fetched 2026-09-23
for departure dates 2022-12-31 → 2026-01-01, and on 2026-09-27 for the extension to
2026-08-31. The builder is `pipeline/scripts/build_fi.py`, the adapter
`berg_pipeline/europe/fi.py`.

## The source

- `GET https://rata.digitraffic.fi/api/v1/trains/{YYYY-MM-DD}` returns every train of one
  departure date — passenger, cargo, shunting, locomotive, test — with its full timetable.
  A 2024 weekday is ~1,760 trains, ~780 KB gzipped, ~2-4 s per request. Send a
  `Digitraffic-User` header. The archive goes back years (at least 2016), so no scraping or
  collection epoch is needed for 2023-2025.
- Timestamps are **UTC ISO-8601 with a `Z`** (`2024-03-12T03:27:59.000Z`). No timezone or DST
  conversion is needed; the adapter parses them as `TIMESTAMPTZ` and stores epoch seconds.
  `departureDate` is the Helsinki operating day, so a UTC day file draws on two service days —
  the builder stages one service day either side of the published window.
- `actualTime` is an observation (track-circuit based). `liveEstimateTime` is a prediction and
  is never read. Measured January 2023: 98.2% of published legs have both ends observed;
  the rest use the timetable on both ends and carry the scheduled-fallback flag.
- Actual times have **second** precision (`timestamp_precision_s: 1`), unlike Ist-Daten's
  minute-precision schedule.

## Shape of a train

- `timeTableRows` is strictly: origin `DEPARTURE`, then `ARRIVAL`/`DEPARTURE` pairs at the same
  station, then the terminus `ARRIVAL`. Stop *k* owns rows 2k-1 and 2k. Verified on every
  staged day: zero violations (`trains_malformed` in `quality.json`). A train that ever breaks
  it is excluded whole and counted, never guessed at.
- The row order **is** a stop sequence. Unlike Ist-Daten, there is no need to order by
  scheduled time, and a delayed train cannot reorder its own stops.
- Roughly half of all rows are timing points the train passes (`trainStopping: false`).
  Only `trainStopping AND commercialStop` stops are published — the same "stations a passenger
  can use" semantics as the Swiss stops. `commercialStop` never disagrees between the two rows
  of one stop.
- Cancellation is two-level: `train.cancelled` (dropped whole) and per-row `cancelled`. A stop
  whose rows are all cancelled disappears, so a train cut short simply terminates at its last
  live stop; nothing is invented for the part that did not run.

## Identity

- **A UIC station code is unique only within a country.** Live metadata has code 1000 as both
  Ahvenus (FI) and Petrozavodsk (RU). The dataset-local station id is the bare code for Finnish
  stations and a country-lifted range for foreign ones (`fi.local_station_id`), and the station
  join always uses `(countryCode, stationUICCode)`.
- The station metadata endpoint is a current snapshot with no history, so the dimension is
  valid for all time. January-February 2023 had zero unmatched stations against it.
- `(departureDate, trainNumber)` is the journey key. The published `trip_id` is
  `"<trainType> <trainNumber>"` (`IC 27`, `HL 9123`), and `line` is the commuter line letter
  (`I`, `Z`) or, for long-distance trains, the same `IC 27` label passengers see.
- Train type codes are **not** comparable with Swiss ones: `S` is a Pendolino here and an
  S-Bahn in Switzerland. The frontend groups services per dataset for exactly this reason.
  Passenger codes seen 2023: `HL` (commuter, ~80% of trains), `IC`, `HDM`, `S`, `HV`, `PYO`
  (night), `MV`, `HLV`, `V`. `HV`, `MV` and `V` are shown as "Other" until their meaning is
  confirmed from Fintraffic documentation.

## Strike days: thin because the railway stopped

Seven UTC days publish almost nothing: 2023-03-20..23, 2023-12-14, 2024-02-02 and 2024-02-12
(8-249 legs, against ~13k normally). The raw responses are complete — ~1,130 passenger trains
each — but **96-99.9% of them carry `cancelled: true`**. These are the Finnish national strikes.
The minimum-leg floor tripped on them exactly as designed, so the check is now source-aware
(`stops.thin_day_verdicts`): a day under the floor passes only if the source itself records
at least half its passenger trains as cancelled. Those days are listed in `quality.json` and in
the manifest's `source_cancelled_days`; any other thin day still fails the build.

## Summer 2026: a reduced Helsinki commuter timetable

June and July 2026 publish about 30% fewer legs than June and July 2025 (268k and 252k against
408k and 352k). The build is not dropping anything. The source's own daily responses list
fewer trains: a weekday has ~860 commuter and long-distance trains instead of ~1,240, and
almost none of them are cancelled. From Saturday 2026-05-30 to Sunday 2026-08-09, the Helsinki
commuter line A is absent and lines K, I and P run at about half their usual count. Long-distance
and the other commuter lines are unchanged. The switches fall on timetable weekends, which is
consistent with a planned reduced timetable rather than a data fault. (Summer 2025 had a milder
cut from late June to early August.)

## The Haparanda link (2026)

The 2026 data serves one Swedish station, Haaparanta pohjoinen (Haparanda norra, SE UIC 10726,
local id 1905010726), from Tornio (UIC 351). It is outside the Finland OSM extract's mapped
track, so its two routes are straight-line fallbacks.

## Build (2026-09-27, extended from 2025-12-31)

The rebuild re-staged the whole window over the existing database. Every 2023-2025 day file
came out byte-identical, so only the 242 new days were uploaded.

| Item | Value |
|---|---:|
| UTC days published | 1,338 (2023-01-01 → 2026-08-30, 0 missing) |
| Published legs | 17,489,126 |
| Scheduled-fallback legs | 404,682 (2.3%) |
| Journeys | 1,414,341 |
| Legs per day | 8 (strike) … 16,169; ~10k-16k normally |
| Registered routes | 1,986 |
| Quarantine | 319 negative_duration, 4 unmatched_station, 4 zero_duration |
| Leg + journey bytes | 166.8 MB (9.54 B/leg incl. sidecar) |
| Published dataset | 167.8 MB of the 200 MB allocation |

Unmatched stations are UIC 392-394, absent from the current metadata snapshot (4 legs).

## Geometry

Built from the Geofabrik Finland extract with `--fit-bbox` (grid and projection fitted to the
served stations, bbox 21.13, 59.73 → 31.04, 67.45). Full registry (2026-09-27): 1,986 routes,
406 of 412 stations snapped (p50 15.2 m, p95 115 m), **16 straight-line fallbacks** (14
unsnappable, 2 unreachable), detour ratio p50 1.09, max 2.06; 194,762 points in 797 KB, built
in 54 s. The five unsnapped Finnish stations (UIC 267, 268, 272, 274, 1343) sit more than
300 m from mapped track; the sixth is Haparanda, beyond the extract.
The Finnish network is one well-mapped component with no multi-gauge stations, which is why it
routes far more cleanly than Switzerland (1,347 fallbacks).
