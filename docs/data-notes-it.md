# Italy data notes

Verified facts about the sources behind `datasets/it/`, measured 2026-09-29. The adapter is
`pipeline/src/berg_pipeline/europe/it.py` (fetching in `it_fetch.py`), and the scripts are
`fetch_it.py`, `census_it.py` and `build_it.py`.

## Sources

- **TrainStats**: <https://trainstats.altervista.org/> (retrieved 2026-09-29). A one-person
  project that polls ViaggiaTreno, the RFI/Trenitalia passenger information system, through
  each day and keeps every train it tracked with its final state. Field documentation:
  <https://github.com/emanu37429/trainstats>.
  - Reuse statement (ReadMe → Disclaimer), verbatim: *"I dati raccolti da questo sito sono
    forniti così come sono, non si dà alcuna garanzia in merito alla loro accuratezza. Tutti
    i dati raccolti possono essere riutilizzati gratuitamente e senza limitazioni, ma è
    gradito un messaggio per informare dell'uso fatto."* (Provided as is, no accuracy
    guarantee; all collected data may be reused free of charge and without restriction; a
    message about the use is appreciated.)
  - The underlying data is ViaggiaTreno's; FS/Trenitalia state no open licence for it. The
    manifest names both.
- **Current archive (Dropbox)**:
  <https://www.dropbox.com/scl/fo/uv5rz6y6gqpkpciyymg0b/ALu4uc7oD0v_iGdFsSYYJZE?rlkey=smo0xhqe4ha7bxjnwfsooagg3>.
  One `dati_YYYY_MM_DD.zip` (one JSON) per day from **2024-06-08**. Single files need a
  Dropbox API app; the folder's `dl=1` link streams everything as one 1.63 GB ZIP, which is
  what `fetch_it.py` downloads.
- **Legacy archive (Mega)**: <https://mega.nz/folder/aQRUAAiY#rGGhCFMaera3oVxDEG3zBQ>,
  through **2024-06-07**, plain JSON (~17 MB a day). `it_fetch.py` reads the public folder
  through Mega's API (AES-decrypted names and contents, pycryptodome). Its folders:

  | Folder | Days | Format |
  |---|---|---|
  | `Vecchio sito/…` (old site) | 2020-01-11 → 2022-02-14, XML dump back to 2019-10-02 | previous database |
  | `dati precedente database (15-05-2022 a 01-11-23)` | 2022-02-15 → 2023-11-01 | previous database |
  | `01_02_2023 a 31_12_2023` | 2023-02-01 → 2023-12-31 | current database |
  | root | 2024-01-01 → 2024-06-07 | current database |

- **Stations**: ViaggiaTreno's own station registry (`elencoStazioni/{0..22}`: code, names,
  coordinates) and name index (`autocompletaStazione/{A-Z,0-9}`), snapshot 2026-09-29; the
  station list of the Figshare dataset 10.6084/m9.figshare.28891607 (CC BY 4.0, 2024); OSM
  railway stations and halts in Italy (Overpass, ODbL), snapshot 2026-09-29. Only derived
  station points are published; none of these files go to R2.

Every fetched day is recorded in `data/datasets/it/raw/trainstats/ledger.csv` with source,
file name, sizes, SHA-256 (of the Dropbox inner ZIP; of the Mega plaintext JSON) and retrieval
time. Raw files never go to R2.

## Why the window starts on 2023-02-01

TrainStats moved to MongoDB on 2023-11-01 and re-exported its history from 2023-02-01 in the
new format: every train has an `_id`, and files are organized by the day a train **departs its
origin**. Before that, files come from the previous database: no `_id`, and a train is filed
under the day it **arrives** (a night train appears in the next day's file). The
`01_02_2023 a 31_12_2023` folder is that re-export, and it overlaps the previous database's
files for 2023-02-01 → 2023-11-01.

So one train format covers 2023-02-01 → today. Earlier years are the previous format, from
TrainStats' least-filtered collection period (its ReadMe, item 3), with FerrovieNord delays
recorded as zero throughout. They would need a second parser and cross-file deduplication
for arrival-day filing. They are not built; the archive is inventoried above for later.

## The archive files

- `{giorno: "DD/MM/YYYY", timeZone, riassunto, avvisiRFI, avvisiTI, treni: [...]}`. The day
  in `giorno` always matches the file name (checked on every extract).
- Stop times are **true UTC epoch seconds**: `801` departing Milano Cadorna at 00:02 CEST on
  2026-09-22 has `op = 1790028120` = 2026-09-21 22:02 UTC. Clock changes therefore need no
  handling in the adapter; the DST fixtures in `tests/test_europe_it.py` pin it.
- Delays are strings: whole minutes, or `N` (the side does not exist), `S` (the stop was
  cancelled), `n.d.` (not detected). A train-level `rp`/`ra` of `X` is a cancelled train.
- The documented "schema change" of **2024-12-29** is only the daily summary block
  (`riassunto`: `ritardoAccumulato` → `ritardoGenerale`, per-category totals). Train and stop
  records are unchanged, so the adapter has one parser. It whitelists every train, stop and
  top-level key and every delay marker, and fails on anything else.
- Stops gained platforms (`br`, `bp`) on **2024-12-25**. `oaz`/`opz` appear on a handful of
  trains (legacy duplicates of `oa`/`op`). `pr` is present exactly on cancelled trains.
- `cn` (number changes, `2378,VOGHERA`) is present in 2023-2024 and absent from 2025 on.
- The summary's `treniCircolati` is Trenitalia's own count of trains that ran; TrainStats
  tracks about 93% of them (`treniMonitorati` minus cancellations).

## Normalization

- A time is **scheduled + reported delay**, never an observation: `time_semantics =
  delay_only`, labelled "scheduled + reported delay" in the UI like the Netherlands. A numeric
  delay reconstructs the event; `n.d.` falls back to the timetable and is flagged; `N` means
  the side has no event (origin arrival, terminus departure) whatever time is printed; a zero
  scheduled time is no event either.
- **Cancellations**: a train with `X` is cancelled outright and publishes nothing, but is
  counted per service day (`source_days`) and kept in the local `it_trains` table. A stop
  marked `S` keeps its row with no times, so the shared leg builder never draws a leg into,
  out of or across it (`not_run`): a train cut short or skipping a section is not bridged.
- **Route changes**: `dl` (free text), `oo`/`od` (planned origin/destination) are kept in
  `it_trains` with a status (`ran`, `part_cancelled`, `cancelled`, `no_stops`).
- **Operational points**: diverted trains list junctions and passing points (`BIVIO …`,
  `PM …`, `POSTO MOVIMENTO …`, `DEVIATOIO …`). A name no station list knows that matches that
  pattern is not a station: the call is dropped and the train's legs join the real stops on
  either side, which it did run between. A real halt with such a name (FER's `BIVIO BARCO`)
  is pinned by an alias.
- **Duplicates**: the same `_id` twice in a day is one train recorded twice; the copy with
  more stops is kept. Two trains with one number where the cancelled one carries the other's
  exact origin and destination times is ViaggiaTreno's duplicate-number bug (TrainStats
  ReadMe, item 2): the cancelled copy is dropped rather than counted as a cancellation.
- **FerrovieNord before 2023-11-04**: TrainStats recorded every delay on the FerrovieNord
  network as 0 until it added a separate collector (changelog 03/11/2023). Those stations are
  found in the data, not listed by hand: every stop name whose delays before the fix are at
  least 99% exactly zero (over at least 100 values). Their delays before 2023-11-04 are
  treated as not detected. The list is written to quality.json. Trains running on both RFI
  and FerrovieNord track are, per TrainStats, reliable only on the RFI part after the fix too.
- A delay outside −30 min … +12 h is a ViaggiaTreno artefact and is treated as not detected.
- **Inconsistent arrivals**: with whole-minute delays, a reconstructed arrival at or before
  the reconstructed departure from the previous stop cannot both be right. That arrival is
  flagged unmeasured, so the hop falls back to the timetable. Without this, 3.5% of hops
  would be quarantined as zero-duration and 1.2% as negative (measured on 2025-06).
- **Journey identity**: `trip_id` is category + number (`REG 2012`, `FR 9519`), with ` #k`
  for a second train of that number on one service day, ordered by departure. `line` adds
  every changed number (`REG 2377/2378`), so search finds a train by either. The service day
  is the archive day, which is the origin's departure day in Rome time.
- **Categories**: `c`, or `sub` when `c` is empty or `ES*` (Frecce: FR, FA, FB). REG,
  MET, IC, ICN, EC, EN, EXP, IR, NCL kept as written; no category at all is `NC`.

## Stations

TrainStats names stations; it has no codes. ViaggiaTreno renamed its stops on **2025-03-27**
(`BOLOGNA C.LE` → `BOLOGNA CENTRALE`, `FIRENZE S.M.N.` → `FIRENZE SANTA MARIA NOVELLA`), and
FerrovieNord's stations carried FN's own names before November 2023 (`M N CADORNA`,
`M.N. AFFORI`). The pre-2025 names are the registry's long names; the backtick stands for an
apostrophe (`FORLI\``).

Neither codes nor list coordinates are reliable identities on their own:

- **Codes are reused.** S00193 is Palermo's `EMS-La Malfa` in the 2024 list and Torino
  Orbassano S.Luigi today; PERCA and BRUNICO NORD swapped codes.
- **Coordinates are wrong** for dozens of stations: 80 registry stations sit near 0°N 0°E,
  and others are 10-400 km off (Ponte di Nona 78 km, Policlinico 330 km).
- **Names are ambiguous**: `ACQUAVIVA` in the name index is Acquaviva delle Fonti (Puglia);
  the trains calling there run Campofranco – Cammarata, which is Acquaviva Platani (Sicily).

So `it.resolve_stations` gives each name a code *and* a point, and trusts a point only when
two independent sources agree within 3 km:

1. Code: the first name layer that knows the name: manual aliases
   (`pipeline/data/it_station_aliases.csv`), the current name index, the registry's long then
   short names, the same names punctuation-blind, then the Figshare list.
2. Confirmed point: an OSM station with any of the code's spellings within 3 km of the
   matching list's point (OSM's point is used), or the registry and Figshare list agreeing.
3. **The trains themselves**: a stop must lie roughly between the stops before and after it
   (through it at most 1.5 × the direct distance + 20 km), or within 120 km of its only placed
   neighbour, in at least half its calls. Confirmed points that fail are demoted, worst first
   (one misplaced station would otherwise drag its neighbours down with it); unplaced names
   take their first candidate that passes, OSM's before the lists'.
4. Identity: names sharing a code are one station unless each is confirmed at a different
   place (the second is the code reused: id + 4,000,000). FerrovieNord's N-codes merge into
   the S-coded station within 0.5 km.

Every decision is written to `data/datasets/it/station_review.csv`.

## Measured quality

Full build of 2026-09-29, 2023-02-01 → 2026-09-26:

- 1,326 published UTC days out of 1,334. The 8 not published are the collection outages and
  source gaps above: 2023-06-04/05, 2023-11-01, 2024-06-08, 2024-12-12/13, 2026-06-28 and
  2026-07-07. Their neighbours' boundary hours are in quality.json's `source_gap_hours`
  (33 hours).
- 110.3M legs and 10.3M journeys. 21.9% of legs use the scheduled fallback: a leg needs a
  measured time at both ends, and 5.4% of stops have none, 3.0% more have an inconsistent
  arrival (above), and FerrovieNord's zero-delay stations before 2023-11-04 (67 stations,
  0.85M calls) are blind.
- 56k-103k legs per ordinary UTC day. Twenty strike days, on which the source's own summary
  cancels at least a quarter of the trains, publish thin (14k on 2024-06-16) and are listed in
  quality.json's `source_quiet_days`.
- 696.6 MB of legs and journeys at zstd 19, 6.32 B/leg including the sidecar and about
  525 KB a day, against an 800 MB allocation.
- Stations: 93.8% of calls resolve to an OSM-confirmed point, 4.1% to the two lists
  agreeing, 1.6% by train context, 0.5% by alias and 0.03% to operational points. 328 calls
  stay unresolved, 78 conflict with their train's context and 67 are unmatched.
- Quarantine over the whole window is small because unmeasured arrivals fall back to the
  timetable: 2,627 zero-duration, 1,158 negative, 839 unmatched station and 499 absurd. A
  further 309k legs leave Italy and are clipped.
- **Inconsistent arrivals double from April 2025**, from about 2.3% of stops to about 4.0%.
  The step follows the 2025-03-27 ViaggiaTreno rename. Its cause is not established; those
  arrivals use the timetable either way.

| Month | Trains | Cancelled | Calls | Measured stops | Legs |
|---|---:|---:|---:|---:|---:|
| 2023-02 | 221,195 | 585 | 2,618,712 | 92% | 2,391,866 |
| 2023-06 | 208,619 | 1,010 | 2,428,764 | 93% | 2,206,840 |
| 2023-12 | 229,480 | 2,341 | 2,688,667 | 96% | 2,428,152 |
| 2024-06 | 223,502 | 2,346 | 2,588,257 | 96% | 2,333,404 |
| 2024-12 | 223,815 | 1,646 | 2,646,011 | 95% | 2,399,749 |
| 2025-06 | 240,101 | 6,558 | 2,814,598 | 95% | 2,491,691 |
| 2025-12 | 251,929 | 5,197 | 3,023,324 | 95% | 2,705,178 |
| 2026-06 | 241,528 | 5,932 | 2,804,328 | 94% | 2,489,905 |
| 2026-09 (to 27) | 230,073 | 5,287 | 2,726,503 | 94% | 2,430,070 |

Every month is in quality.json's `months`. Months are service days as staged, one day either
side of the window included.

## Geometry

The rail extract and commands are in `geometry/README.md`. Results:

- 12,157 routes over 2,515 stations. 2,504 stations snapped (p95 24 m). The detour ratio is
  p50 1.08 and p99 1.86. 3.2 MB.
- 101 straight-line fallbacks, all listed in `routes.report.json` as `fallback_pairs`:
  - 62 involve the 11 stations that did not snap to OSM track: Ospitaletto, Castellucchio,
    S.Michele in Bosco, Mattarello, Melzo Scalo, Montorsoli, Valle di Maddaloni, Napoli
    Afragola PES, Maddaloni Superiore, San Leonardo di Cutro and Porto Empedocle.
  - 25 are unreachable. Six are self-pairs with no length (Busto Arsizio, Brunico Nord,
    Venezia Mestre, S. Vito al Tagliamento, Genova Piazza Principe, Olbia), and Bozzolo –
    Marcaria has no connected track. The rest cross the Strait of Messina, where rail never
    crosses in OSM: Villa San Giovanni – Messina, and Intercity and night trains to Sicily.
  - 14 are rejected detours (7 pairs, both directions): Bergamo – Seriate, Montello and
    Grumello; Trieste Centrale – Villa Opicina; Lugo – S.Agata; and Acerra – Cancello and
    Afragola.
- Build time: staging and legs about 10 minutes for the whole window (4 extract workers),
  geometry about 4 minutes.
