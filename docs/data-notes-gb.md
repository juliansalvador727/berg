# Great Britain data notes

Verified facts about the sources behind `datasets/gb/`, measured 2026-09-28. The adapter is
`pipeline/src/berg_pipeline/europe/gb.py`, and the build is `scripts/build_gb.py`.

## Why not HSP

The brief for this dataset named National Rail's Historic Service Performance (HSP) API. It
was not used, for three reasons:

- **HSP cannot list a day's services.** `serviceMetrics` answers only for a station pair
  (`from_loc`, `to_loc`) in a time band, and `serviceDetails` takes one RID at a time. A
  national day would need thousands of pair queries, then a detail request for each of the
  ~22,000 trains, with no way to prove the pair list is complete.
- It needs a Rail Data Marketplace account, and none exists here.
- It keeps about a year and gives nothing HSP-specific that Darwin itself does not carry.

HSP is fed by Darwin. An archive of Darwin's own output stream answers the same questions,
covers every passenger train by construction, and downloads without credentials.

## Sources

- **Darwin Push Port archive**: <https://ilovetrains.co.uk/darwin-push-port>. David
  Wheatley records the Rail Data Marketplace's Push Port product
  (<https://raildata.org.uk/dashboard/dataProduct/P-d3bf124c-1058-4040-8a62-87181a877d59/overview>)
  and publishes it as one gzipped file per hour:
  `https://ilovetrains.co.uk/api/download-darwin-dump?fileName=YYYY%2FMM%2FDD%2FHH.pport.gz`.
  - It was announced on openraildata-talk ("Darwin Historic Data") and starts on
    **2025-09-07**. The first day is partial: 110 MB against about 210 MB for a weekday.
  - It was current to the day on 2026-09-28. Unlike HSP, nothing is known to expire.
  - Darwin's terms of access are OGL v2.0 with NRE amendments and require attribution to
    National Rail Enquiries. The archive itself states no licence.
- **Stations**: NaPTAN, DfT, OGL v3.0,
  <https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv> (102 MB, all modes).

## The archive files

- One Pport XML document per line, in arrival order. A file is named after the **local**
  date and hour in the message's own `ts` attribute (`2025-09-08T08:00:00.03+01:00` is in
  `…/2025/09/08/08.pport.gz`).
- A weekday hour is about 10 MB gzipped (118 MB of XML), and a whole weekday about 210 MB.
  The year is about 80 GB. A single connection downloads an hour in about 5 s. The build
  fetches four days at a time, and one day in about 35 s.
- Message types in one weekday hour: ~120k `TS` (train status), ~1.8k `deactivated`, ~1.3k
  `formationLoading`, and ~1.5k `schedule`.
- **The timetable for service day D loads between 02:00 and 04:00 on D**: 28k and 17k
  `schedule` messages in D's 02 and 03 files, then 1-2k an hour of changes. A few hundred
  schedules for D arrive on D-1, so staging D reads the extracts of D-1, D and D+1. D+1 is
  needed for trains past midnight.
- Every `schedule` message is a whole service: `rid` (unique, date-prefixed), `uid`,
  `trainId` (headcode), `ssd` (start date), `toc`, and optional `trainCat` (default `OO`),
  `status` (default `P`), `isPassengerSvc` (default true) and `deleted`. The locations are
  `OR`/`IP`/`PP`/`DT` and the operational `OPOR`/`OPIP`/`OPDT`, each with working times
  (`wta`/`wtd`/`wtp`) and, at public calls, public times (`pta`/`ptd`). `can="true"` marks a
  cancelled call. The latest message for a `rid` replaces all earlier ones.
- `TS` messages identify a location by `tpl` plus its working times, the same key the
  schedule uses. This keeps two visits of a circular train to one station apart. The
  arrival, departure and pass children carry `at` (actual), `et`/`wet` (forecasts, never
  used), or `atRemoved="true"` when an actual is withdrawn. Actuals are **whole minutes**.
  `atClass` is `Automatic` (track circuits), `GPS` or `Manual`.

## Normalization

- Passenger services have status `P` or `1`, have `isPassengerSvc` not false, and are not
  in category `EE`/`EL`/`ES` (empty stock), `BR`/`BS` (buses) or `SS` (ship). Also dropped
  are London Underground (`LT`), Tyne and Wear Metro (`TW`), Sheffield Supertram tram-trains
  (`SJ`) and the North Yorkshire Moors Railway (`NY`). These are in Darwin, but their stations
  are mostly missing from NaPTAN's rail list, so they would only half-render.
- Only public calls (`pta` or `ptd` present) become stops. Passing points and working-only
  stops do not.
- A cancelled call keeps its row with no times, so the leg builder never bridges a
  part-cancelled train over the gap. Wholly cancelled services are counted per day in
  `source_days` and in quality.json's stage stats.
- Midnight: working times are unwrapped along the schedule. A day is added when the clock
  falls more than 12 h behind the previous location. Each public or actual time is then put
  on the day nearest its location's working time. A service keeps its `ssd` as the service
  day, however late it runs.
- **Clock changes.** Darwin writes the two nights differently, and both were checked on
  Thameslink trains that run through them:
  - Autumn, 9W18 Brighton 23:23 → Bedford on 2025-10-25: the timetable stays in BST through
    the change. Harlington 01:58, Flitwick 02:02 and Bedford 02:14 are one continuous clock.
  - Spring, Bedford 23:36 → Brighton on 2026-03-28: the timetable is wall-clock. St Pancras
    is at 00:54 GMT, then Blackfriars at 02:02 BST, 8 minutes later.
  - Actuals are the wall clock on both nights: 01:57 at Harlington before the change and
    01:10 at Bedford after it, and 03:02 BST at Gatwick.

  So autumn timetables use the start day's offset, and spring timetables use ordinary
  wall-clock conversion. On either night, each actual uses whichever offset lands it nearer
  its scheduled instant. A per-timestamp time-zone lookup put Bedford an hour early, and the
  start-day offset alone made St Pancras → Blackfriars take 68 minutes.
- Two consecutive calls at one TIPLOC, from a train reversing or dividing (Virginia Water,
  Leeds, Cardiff…), become one stop. The first call gives the arrival and the second gives the
  departure, so the train is never drawn as a leg from a station to itself.
- Category (the wire's train type) is the operator code (`VT`, `GW`, …). Darwin's `trainCat`
  is `XX`/`OO` for almost everything and says nothing to a viewer. The journey sidecar's
  `line` is operator + headcode, and `trip_id` is the RID.

## Stations and geometry

- NaPTAN rail access nodes have ATCO code `9100` + TIPLOC, which is exactly what Darwin
  calls at. The station id is the TIPLOC read as a base-37 number: deterministic, reversible,
  below 2^53.
- Inactive NaPTAN rows are kept, because Darwin still uses superseded TIPLOCs (`STPANCI`,
  `STFORDI`, `EBSFLTI`).
- Eleven active rows, including the Elizabeth line core (`PADTLL`, `TOTCTRD`, `BONDST`, …)
  and Barking Riverside, have an OS grid reference but no latitude/longitude. They are placed
  from the nearest geocoded station by the grid offset in metres, which is good to a few
  metres at London distances.
- Five TIPLOCs that NaPTAN files under another code of the same station are aliased
  (`TIPLOC_ALIASES`).
- One NaPTAN point is 315 m from the track, beyond the geometry job's 300 m snap ceiling:
  Glazebrook, which sits on the station approach road. `COORDINATE_OVERRIDES` moves it onto
  the line, measured from OSM. Without the override, Irlam–Glazebrook–Birchwood (15.7k legs)
  drew as straight lines.
- Result on 2025-09-08: 274,252 of 274,268 staged calls match, and the rest are Eurostar's
  continental calls. Those are clipped as outside the country, so Eurostar ends at the
  tunnel.

## Measured sample (2025-09-08 → 2025-09-30)

- Passenger services per service day: about 21,900 on weekdays, about 20,000 on Saturdays
  and about 12,700 on Sundays. Wholly cancelled: 150-730 a day. Part cancelled: about 650 a
  day.
- About 86% of public calls have an actual arrival or departure.
- Legs: about 243k per weekday. 4.2% use the scheduled fallback.
- Quarantine: zero duration 1.2% (whole-minute actuals on short hops), negative 0.25%.
- 5.55 bytes per leg including the journey sidecar, at zstd 19. That is about 1.34 MB per
  weekday.
- Corridors on 2025-09-10 (trains, median observed vs scheduled minutes, median arrival
  delay):

  | Corridor | Trains | Observed | Scheduled | Delay |
  |---|---|---|---|---|
  | Euston → Birmingham New St | 78 | 105 | 92 | +4 |
  | Euston → Manchester Piccadilly | 46 | 138 | 131 | +9.5 |
  | King's Cross → Edinburgh | 31 | 268 | 265 | −2 |
  | Paddington → Bristol TM | 35 | 95 | 95 | +1 |
  | Victoria → Brighton | 40 | 63 | 58 | +2 |
  | King's Cross → Cambridge | 67 | 60 | 53 | 0 |
  | Manchester Victoria → Leeds | 89 | — | — | — |
  | Glasgow QS → Edinburgh | 46 | 50 | 51 | 0 |

  TransPennine's Manchester–Leeds trains use Victoria; only 4 run from Piccadilly.
- One train was checked call by call against the raw messages: 1H34 Euston 17:13 →
  Manchester, arriving 19:39 against 19:19.

## Archive gaps

HTTP 404 hours, recorded as `.missing` markers next to the raw files:

- 2025-11-17, 08:00-16:00 except 13:00.
- 2026-03-09 11:00 → 2026-03-10 07:00, which includes 03-10's timetable load.
- 2026-03-21, 03:00-14:00, which includes half of that day's load.
- 2026-05-15, 04:00-05:00 BST.
- 2026-03-29 01:00, which is not a gap: that local hour does not exist.

The consequences:

- **A service day whose 02:00 or 03:00 file is missing is not published.** Its timetable
  cannot be rebuilt from later changes. 2026-03-10 and 2026-03-21 are therefore in the
  manifest's `missing_days`, and the next UTC day's first three hours (those days' trains past
  midnight) are listed as gaps.
- Any other missing hour loses only actuals and late changes. Its trains run on the
  timetable, flagged as scheduled fallback, and the UTC hour is listed in the manifest's
  `source_gap_hours`: 29 hours in total.

## Build (2026-09-29)

- Coverage 2025-09-08 → 2026-09-26: 382 published UTC days, 2 missing (above).
- Christmas Day (284 legs) and Boxing Day (18,956) are the railway's own quiet days: the
  timetable load is intact and nearly empty. They are published below the 20k-leg floor and
  listed in quality.json's `source_quiet_days`.
- 84.78M legs and 7.58M journeys; 4.6% of legs use the scheduled fallback.
- 477.6 MB of legs and journeys at zstd 19 (5.63 B/leg incl. sidecar), about 1.25 MB a day,
  against a 650 MB allocation. The bucket went from 5.63 GB to 6.11 GB.
- Quarantine over the whole window:

  | Reason | Legs |
  |---|---:|
  | Zero duration (whole-minute actuals on short hops) | 890k |
  | Negative duration | 188k |
  | Unmatched station (Eurostar abroad) | 7.0k |
  | Missing time | 3.2k |
  | Absurd duration | 25 |

- Geometry: 11,368 routes, 0 straight-line fallbacks, all 2,645 stations snapped. The
  detour ratio is p50 1.06 and p99 1.60. The worst ratios are real railway: Liskeard ↔
  Coombe Junction reverses, Askam ↔ Millom goes round the Duddon estuary, and Mitcham
  Junction ↔ Morden South is the Wimbledon loop.
- Build time: extraction about 96 s per archive day per core (5 in parallel); staging and
  legs for the whole year about 15 minutes; geometry about 3 minutes.

| Month | Services | Cancelled | Part-cancelled | Calls | Obs. arr | Obs. dep | Legs | Zero dur | Neg dur |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2025-09 (from 07) | 484,339 | 7,622 | 14,809 | 6,015,983 | 87% | 87% | 5,303,825 | 60,572 | 12,063 |
| 2025-10 | 636,272 | 13,592 | 19,438 | 7,915,526 | 87% | 86% | 6,943,345 | 77,636 | 17,375 |
| 2025-11 | 602,808 | 12,531 | 21,228 | 7,510,522 | 84% | 84% | 6,585,032 | 68,960 | 16,057 |
| 2025-12 | 584,732 | 13,879 | 19,896 | 7,306,962 | 86% | 86% | 6,391,367 | 65,420 | 15,104 |
| 2026-01 | 629,884 | 14,607 | 19,763 | 7,807,373 | 87% | 86% | 6,854,206 | 71,298 | 15,374 |
| 2026-02 | 573,791 | 9,309 | 15,851 | 7,108,725 | 87% | 87% | 6,287,997 | 65,486 | 13,967 |
| 2026-03 | 605,233 | 11,435 | 17,667 | 7,530,435 | 85% | 85% | 6,653,831 | 67,814 | 14,089 |
| 2026-04 | 619,509 | 10,557 | 17,406 | 7,723,120 | 87% | 87% | 6,830,867 | 73,505 | 14,941 |
| 2026-05 | 629,995 | 14,989 | 22,142 | 7,793,656 | 86% | 86% | 6,815,329 | 68,400 | 14,918 |
| 2026-06 | 622,771 | 22,211 | 24,148 | 7,754,849 | 85% | 85% | 6,702,219 | 75,367 | 13,879 |
| 2026-07 | 644,390 | 20,286 | 26,396 | 8,100,155 | 85% | 85% | 7,041,275 | 70,695 | 14,814 |
| 2026-08 | 630,794 | 22,063 | 26,245 | 7,850,560 | 85% | 84% | 6,767,514 | 65,449 | 12,884 |
| 2026-09 (to 27) | 556,125 | 12,347 | 19,732 | 6,956,998 | 86% | 86% | 6,102,412 | 59,750 | 12,561 |

Months are service days as staged, one day either side of the window included. Cancelled means
every public call was cancelled; those journeys have no movement to draw and exist only in
these counts. Berg's wire has no cancellation layer yet.
