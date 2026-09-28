# Austria data notes

Verified facts about the two ÖBB sources behind `datasets/at/`, measured 2026-09-25. The
adapter is `pipeline/src/berg_pipeline/europe/at.py`, and the build is `scripts/build_at.py`.

## Sources

- **Train runs ("Zugfahrten")**: ÖBB-Infrastruktur AG, CC BY 3.0 AT, attribution "Datenquelle:
  ÖBB-Infrastruktur AG". <https://static.web.oebb.at/open-data/infra/delVO_mmtis/mmtis_zugfahrten.zip>
  is linked from <https://data.oebb.at/de/datensaetze~datenbereitstellung_delegierte_verordnung_eu_2024-490~>.
  It is ÖBB's answer to EU Delegated Regulation 2024/490, which requires historic rail delay
  and cancellation data on the comprehensive TEN-T network from 2025-12-01.
- **Timetable**: ÖBB-Personenverkehr AG GTFS, CC BY 4.0.
  <https://static.web.oebb.at/open-data/soll-fahrplan-gtfs/GTFS_Fahrplan_2026.zip> covers
  ÖBB-Personenverkehr, Montafonerbahn and City Airport Train, valid 2025-12-14 → 2026-12-12.
  There is no `GTFS_Fahrplan_2025.zip` (404).

## The train-run file

- It is one rolling ZIP, updated at least weekly (the 2026-09-14 snapshot was last modified
  that day). It holds one set of three CSVs per week, starting with the week of 2025-11-17:
  - `*_mmtis_zugfahrten.csv`: every train run;
  - `*_ausgefallen.csv`: cancelled runs;
  - `*_verspaetet.csv`: runs at least 60 minutes late.

  The build keeps every weekly CSV it has ever downloaded, and every snapshot ZIP, under
  `data/datasets/at/raw/`, in case old weeks drop out of the ZIP.
- The snapshot of 2026-09-25 covers service days 2025-11-17 → 2026-09-06: 294 days with none
  missing, and about 5,560 runs a day.
- A row is one run: train number, operating day, then planned and actual times at the **first
  and last operating point ÖBB recorded**. Times are to the second, with a `+0100`/`+0200` offset.
- **Those points are not passenger termini.** They are operating points: yards (`Ow`, Wien
  Westbf Fbf; `Hfg`, Wien Hütteldorf Güterzuggruppe), junctions (`Mlx`, Wien
  Matzleinsdorf-Laxenburg), track groups (`Nbn`, Wiener Neustadt Hbf-Gleisgruppe 200) and
  station tracks (`Wf H1`, `Wogh1`, with or without the space). Matching both endpoints to a
  timetable trip's first and last stop succeeded for 54 of 25,914 train/endpoint combinations.
  The points are therefore used only as places where the delay was observed.
- **The file includes non-passenger runs**: empty stock and freight in the 30000-49999 and
  50000+ ranges, and trains of operators that are not in the ÖBB timetable.
- **Some actual times are not running times.** A few trains are registered hours before
  their planned start (actual 00:08 for a planned 05:46). A few carry a date days earlier:
  3414 on 2026-02-12 has an actual start of 2026-02-03, and 4832 is three days early.
  - Early departures fall off smoothly to about −10 min, which is timetable padding.
  - Beyond that, 924 departures and 592 arrivals are more than 20 minutes early.
  - 3 observations are more than 12 hours late.

  An observation outside −20 min … +12 h is discarded, and the run's other observation is
  used instead.

## Joining runs to the timetable

- A timetable trip's train number is the trailing number of `trip_short_name` (`RJX 658`).
  Trips active on a day come from `calendar.txt` plus `calendar_dates.txt`.
- Runs and trips are joined on (number, operating day). Where a number has several trips or
  runs that day (`REX 7` in the morning and the evening, or split portions), the one-to-one
  pairing with the greatest overlap of scheduled time spans wins. A pair must overlap, or come
  within 15 minutes.
- **Stop times.** The run's first point gives delay dA at planned time tA, and its last point
  gives dB at tB. A stop scheduled at s gets:
  - dA if s is before tA;
  - dB if s is after tB;
  - the linear interpolation in between.

  The dataset's `time_semantics` is `delay_interpolated`, which the UI shows as
  "scheduled + interpolated delay". It ranks between `delay_only` and `scheduled` for
  cross-border dedup.
- Timetable rows with no pickup and no drop-off are pass-throughs and are dropped. A dwell the
  interpolation would make negative is clamped to zero.
- **Only timetabled passenger trips that ÖBB recorded running are published.** A trip with no
  run may have been cancelled, replaced by buses or run on a private network, and is not drawn.

| Month | Runs paired | Trips paired |
|---|---:|---:|
| 2025-12 (from 12-14) | 91.5% | 86.8% |
| 2026-01 | 91.1% | 83.5% |
| 2026-04 | 87.5% | 84.1% |
| 2026-07 | 80.8% | 78.7% |
| 2026-08 | 79.4% | 76.7% |

The drop in summer is the timetable's age. The GTFS was published once, on 2025-12-12, and
never updated, so construction-period trains that run under other numbers from spring onward
have no trip to join, and the trips they replace have no run. Both are left out rather than
guessed.

## Stations and geometry

- Station ids come from IFOPT stop places: `at:RR:NNNN` becomes RR × 100000 + NNNN, so they
  read back. A foreign stop place gets a stable CRC-based id above every Austrian one, with its
  country from the IFOPT prefix. That way the country clip removes the foreign legs of EC, RJX
  and NJ trains (229,545 `outside_country`) instead of quarantining them.
- There are 1,160 stop places (1,046 Austrian), and every staged stop matched one.
- The geometry input is the Overpass rail export of Austria, merged with 444 ways that OSM
  tags `railway=construction` + `construction=rail` in 2026, retagged as rail. Without them the
  Wien S-Bahn Stammstrecke (Wien Mitte, Rennweg, Quartier Belvedere) and Feldkirch – Buchs
  (Tisis, Gisingen, Altenstadt) cannot be snapped, although trains ran there in the window.
  With them, all 1,018 served stations snap and 0 of 2,875 routes fall back to a straight line.

## Build (2026-09-25)

| Item | Value |
|---|---:|
| UTC days published | 265 (2025-12-15 → 2026-09-05, 0 missing) |
| Train runs / timetabled trips | 1,485,987 / 1,555,817 |
| Paired / published trips | 1,289,422 / 1,279,735 (9,687 had no plausible observation) |
| Published legs | 15,983,394 |
| Journeys | 1,254,691 |
| Legs per day | 48,147 … 67,310 (summer holidays are the low end) |
| Registered routes | 2,875 (0 fallbacks) |
| Quarantine | 1 negative_duration |
| Leg + journey bytes | 130.9 MB (8.19 B/leg incl. sidecar, zstd 19) |
| Allocation | 200 MB |

The window starts at 2025-12-15, not with the runs on 2025-11-17, because no timetable before
the 2025-12-14 change is published. It ends at 2026-09-05, the last UTC day whose following
service day is in the run file.
