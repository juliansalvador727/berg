# Netherlands (Rijden de Treinen): what the data actually does

Dataset `nl`, published under `datasets/nl/` beside the Swiss root. Source: the Rijden de
Treinen train archive, licence CC BY 4.0, attribution "Source: Rijden de Treinen
(rijdendetreinen.nl), CC BY 4.0". Every claim below was measured against the archive files
fetched 2026-09-23 for service days 2022-12-01 → 2026-01-31. The builder is
`pipeline/scripts/build_nl.py` and the adapter is `berg_pipeline/europe/nl.py`.

## The source

- `https://opendata.rijdendetreinen.nl/public/services/services-YYYY-MM.csv.gz`: about 30 MB
  per month, about 1.75M stop rows and 180-200k services. **2023 onward is monthly. 2019-2022 is
  only published as yearly files** (`services-2022.csv.gz`, 390 MB), so the leading
  2022-12-31 service day is split out of the yearly file locally (`nl.fetch_month`).
- One row per stop. Times are **scheduled** RFC 3339 times with the local offset
  (`2023-01-01T02:00:00+01:00`). Delays are **whole minutes**, and they are the last value
  NS's realtime feed reported. The archive never publishes an absolute actual time, and a
  delay is present on every scheduled side, so a genuine zero cannot be told apart from a
  train that never reported: 67.7% of published legs have delay 0. The dataset's
  `time_semantics` is therefore `delay_only`, not `observed`. The UI labels it
  "scheduled + reported delay". The published time is scheduled + delay × 60 s, with
  `timestamp_precision_s: 60`. No leg uses the scheduled-fallback flag because no side lacks
  a delay.
- `Service:Date` is an **operating day**. A service that departs 23:59 and arrives 02:00 keeps
  the earlier date: 3,942 services in 2023-01 start on a later calendar date. The builder
  stages one service day either side of the window.
- Stations: `stations-2023-09.csv` is the newest list, with 591 rows keyed 1:1 by code and
  UIC. It includes 194 foreign stations (D, B, F, A, I, CH, S, GB). The local station id is
  the UIC code (`station_namespace: uic`).

## Service types

`Service:Type` holds Dutch display names, and they drift over time. For example, `Stopbus
i.p.v. trein` became `Stopbus ipv trein` on 2023-05-22, `stoptrein` also appears in
lowercase, `Thalys` became `Eurostar` in 2023-10, and `ICE` split from `ICE International`
on 2025-10-01. Replacement services (bus, metro, tram and taxi, which is about 10% of
services) are matched by `bus|ipv|i.p.v.` and dropped. Trains are mapped to short codes:
SPR (Sprinter), ST (stoptrein), SNT (sneltrein), IC, ICD (Intercity direct), EC, ECD, ICE,
THA, EST, INT, NJ, ES (European Sleeper), NT (nachttrein), EXTRA, SPEC and STOOM.

## Three things that would draw the wrong trains

1. **One train, several records.** The archive stores coupled portions and renumbered
   trains as separate services. For example, IC 2452 becomes 3563 at Schiphol and is
   recorded as a 2452→3563 service, a 3563-only service and a `302452` re-issue, all on the
   same track at the same minute. Train numbers cannot match these records, because the
   duplicates carry different numbers. A service is dropped when every (station, scheduled
   time) it has also appears in one other service that day, and the other service is larger,
   or the same size and issued earlier. This dropped **202,651 records (3.2%)**. Portions that
   separate after running coupled are kept as two services, because they are two trains after
   the split. They leave 120k (route, departure-second) groups with more than one leg, about
   0.25% of published legs.
2. **Diversions append stops out of order.** A rerouted train gets its new stops appended
   with later `Stop:RDT-ID`s, and the original route is sometimes **not** marked cancelled.
   For example, IC 588 is listed as UT, GD, RTA, RTD, then WD, GDG, NWK, CPS, RTN. Ordering by
   id zigzags, and ordering by time interleaves two routes. A service whose id order disagrees
   with its scheduled order, or that visits a station twice, is excluded whole: **19,355
   services (0.3%)**.
3. **Mid-route cancellation.** Arrival and departure are cancelled separately. The adapter
   nulls a cancelled side, and a stop with neither side left is dropped. The shared stop
   builder only forms a hop from a stop that has a departure to one that has an arrival.
   A train cut short and resumed later therefore gets no leg over the part that did not run.
   These hops are counted as the clip verdict `not_run` (4,254) and are not quarantined as
   missing time. The rule lives in `stops.build_legs`, so any future Finnish rebuild gets it
   too.

## Geography

Only legs between two Dutch stations are published (`countries=("NL",)`, clip verdict
`outside_country`). The bounding box alone would not do this, because a box around the
Netherlands also contains Antwerp, Aachen and Cologne. Cross-border legs such as Venlo →
Kaldenkirchen and Roosendaal → Antwerpen are clipped. They belong to cross-border geometry and
journey linking (europe.md phase 4). Clipped: 2.13M `outside_country` plus 1.17M
`outside_bbox` legs.

Unmatched stations (6,530 legs) are **all foreign**: Baden-Baden, Osnabrück Altstadt,
Düsseldorf-Bilk, Prague, Berlin, Dresden, and so on. The one Dutch code among them is the SSN
steam depot (RTNG, heritage). These legs would have been clipped anyway, so no Dutch passenger
station is missing from the 2023-09 list.

## Build (2026-09-23)

| Item | Value |
|---|---:|
| UTC days published | 1,096 (2023-01-01 → 2025-12-31, 0 missing) |
| Published legs | 48,709,956 |
| Journeys | 5,912,289 |
| Legs per day | 15,329 … 52,597 (holidays are the low end) |
| Registered routes | 2,189 |
| Cancelled services dropped | 147,477 |
| Quarantine | 5,737 negative_duration, 5,269 zero_duration, 6,530 unmatched_station, 3 absurd_duration |
| Leg + journey bytes | 284.4 MB (5.84 B/leg incl. sidecar) |
| Allocation | 500 MB |

Legs by type: SPR 25.1M, ST 11.6M, IC 10.6M, SNT 0.86M, ICD 0.42M, and international and
special trains below 50k each.

The Dutch payload costs fewer bytes per leg than Finland (9.5) because minute-precision
delays repeat heavily and compress well.
