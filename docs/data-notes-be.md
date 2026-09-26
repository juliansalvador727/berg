# Belgium (Infrabel): what the data actually does

Dataset `be`, published under `datasets/be/` beside the Swiss root. The source is Infrabel's
monthly raw punctuality files, licence CC0, attribution "Source: Infrabel
(opendata.infrabel.be), CC0". Every claim below was measured on the files fetched 2026-09-24
for service days 2022-12-31 → 2026-01-01. The builder is `pipeline/scripts/build_be.py`. The
adapter is `berg_pipeline/europe/be.py`.

## The source

- Monthly files are at
  `https://fr.ftp.opendatasoft.com/infrabel/PunctualityHistory/Data_raw_punctuality_YYYYMM.csv`,
  listed by the dataset `stiptheid-gegevens-maandelijksebestanden`. Each is about 330 MB of
  uncompressed CSV holding 1.9-2.1M rows, and the whole window is 12.6 GB.
  - The server gives each connection a few hundred KB/s and ignores `Accept-Encoding`. Fetching
    therefore runs 16-20 months in parallel, which reached about 2.8 MB/s in total, and resumes
    partial `.part` files with Range requests.
  - A plain `curl` to the final filename leaves a truncated file that looks complete, so the
    fetcher never writes to the final name directly.
- There is one row per **measuring point** a passenger train passed or served on Infrabel's
  network. Each row has a planned and an actual time **to the second**, in Brussels local time,
  and each side carries its own date column. `DATDEP` is the operating day, and a train crossing
  midnight keeps it. Infrabel's own train detection supplies the actual times, so the dataset is
  `observed`.
- **Only trains that ran are listed.** Every planned side has an actual. Cancelled trains and
  cancelled stops are absent, not flagged. There is therefore no cancellation capability. The
  thin-day check cannot be excused by the source, so `source_days` records zero cancellations.
- **Only Infrabel's network is covered.** An ICE from Frankfurt appears from Hergenrath at the
  border. Its German stops are not in the file. The border point is the train's first or last
  row, coded NULL like any origin or terminus, and it is kept, so the train is drawn up to the
  border (Hergenrath, Quévy, Arlon) even though no passenger boards there. Trains to Roosendaal
  and Maastricht end at Essen and Visé, their last Belgian stops.
- Stations are keyed by `PTCAR_NO`, which is the `ptcarid` of the dataset
  `operationele-punten-van-het-netwerk` (also CC0), and the local station id is that ptcarid
  (`station_namespace: be-ptcar`). The ptcar list is a current snapshot of 1,359 points: 750
  stations or halts, plus junctions, sidings and workshops. The export has a UTF-8 BOM and
  `lat, lon` in a single field.

## Column layout changes: read by name

- 2014-01 → 2025-07 share one 21-column layout.
- **2025-08 reorders the columns** (`DATDEP,CIRC_TYP,TRAIN_NO,...`).
- **2025-09 adds `OP1_COD`**, for 22 columns.
- `read_csv` over a list of files maps columns **by position** unless `union_by_name=true`.
  Without that flag, a build spanning 2025-07/08 would silently shift every field. The adapter
  reads by name and checks only the columns it uses.
- `THOP1_COD` keeps the same value distribution across the change. It is still the stop code
  in 2025-09+ (`OP1_COD` disagrees with it on about 15% of rows and is unused).

## What is a stop

`THOP1_COD`, with shares measured on 2025-07:

| Code | Share | Meaning |
|---|---:|---|
| `=` | 49% | Commercial stop, even with zero planned dwell |
| `D` | 28% | Run through (halts a train doesn't serve) |
| `P` | 13% | Run through at a station |
| NULL | 10% | Origin and terminus, **and every stop of an `EXTRA` train** |
| `V` `C` `(` `)` `<` `>` | <0.3% | Coupling, splitting and reversals, all with real dwell times at real stations |

The adapter drops `D` and `P` and keeps everything else. It also drops stops at ptcars that are
not a `Station` or a `Stop in open track`. One example is `BALEN-WERKPLAATSEN`, a workshop
coded `>`. Out of 41.08M stopping rows, 9,330 were dropped this way.

NULL also appears mid-route on trains the source re-planned during a disruption. Those points
cannot be told apart from stops and are kept. The train still passed them, so the geometry is
right either way.

## Categories

The category comes from the prefix of `RELATION`, and the whole relation (`IC 03`, `L B1-1`) is
kept as the line:

- IC, L (local) and P (peak-hour) are domestic.
- EXTRA and CHARTER are special trains.
- ICE, THA (source `THAL`), EST (`EURST`), TGV and INT are international.
- **Suburban S lines**: until the 2025-12-14 timetable they are filed as local trains with a
  network letter: `L B1-1` is Brussels S1, and A, C, G and L are Antwerp, Charleroi, Ghent and
  Liège. From that timetable onward the source names them directly (`S1-2`, `S61-1`). Both
  map to `S`, and plain `L 15` stays `L`.
- One train (2023-08-23, number 22221) has no relation and gets the generic `TRAIN`.

## Rules that keep the drawing honest

1. **One train number, one run per day.** A train that serves a station twice, or whose planned
   arrival precedes the previous departure, is excluded whole: **2,223 of 3.63M train-days
   (0.06%)**. The cases inspected were disruptions, for example an IC turned back at Ruisbroek
   and rerouted via Puurs.
2. **Ties in the planned minute.** Neighbouring halts can share a planned minute (1,348 cases).
   Ordering by planned time alone was non-deterministic: two identical rebuilds differed by a
   few legs and route pairs. Ties are broken by the actual time, which is the order the train
   really passed them, and then by the ptcar id. Two consecutive rebuilds now produce identical
   legs, bytes and `route_pairs.json`. The tie-break also removed about 500
   negative-duration legs.
3. **Stations closed before the snapshot.** Two halts served in 2023-2025 are missing from
   today's ptcar list, and `be.CLOSED_STATIONS` supplies them:
   - Mortsel-Deurnesteenweg (864): matched by TAF/TAP code `BE00864` in iRail's
     `stations.csv`.
   - Baulers (125): in no Belgian list any more, so it comes from Wikidata Q109037563.

   Both lie within 30 m of an OSM rail node. OSM's "Station de Baulers" way sits 1.3 km from
   any track and is not the halt. After the supplement, no stop is unmatched.

## Build (2026-09-24, extended 2026-09-25)

| Item | Value |
|---|---:|
| UTC days published | 1,338 (2023-01-01 → 2026-08-30, 0 missing) |
| Published legs | 46,017,097 |
| Journeys | 4,222,195 |
| Legs per day | 7,166 … 44,226 |
| Registered routes | 3,859 (0 straight-line fallbacks, 666/666 stations snapped) |
| Quarantine | 8,082 negative_duration, 63 zero_duration, 2 absurd_duration |
| Clipped as not_run | 235 |
| Leg + journey bytes | 401.0 MB (8.71 B/leg incl. sidecar) |
| Allocation | 450 MB (was 350 MB for 2023-2025) |

The 2026-09-25 extension re-ran the whole build over the existing database. The 2026 files keep
the 2025-09 column layout, and every 2026 stop matched a ptcar. The append-only registries kept
every route and type id, and all 2023-2025 day files came out byte-identical. The 260 new
station pairs route with no fallback.

Legs by type (2023-2025): S 15.8M, IC 12.3M, L 6.8M, P 1.75M and EXTRA 0.63M. International types have
under 31k legs each, because most make one Belgian stop before the border: TGV contributes
only 327.

The thinnest days are national rail strikes, not ingest failures: 2025-01-13 (7,166 legs), the
strike week of 2025-02-22 → 03-02, and 2025-11-24/25. All of them are above the 5,000-leg
floor. A normal weekday is 38-44k legs and a weekend about 22k.

**Storage.** Second-precision times on short hops cost about 9.5 B/leg at DuckDB's default
zstd level, which projected to about 390 MB, over the cap. Following europe.md's remedy
order, the dataset sets `compression_level=19`. That makes a day file 10% smaller, and levels
above 19 gained nothing. Doubling the row groups would have saved another 7%, but row-group
size is the measured browser range-read sweet spot, so it was left alone. Every other dataset
keeps the default level, and their files are byte-identical.

Share of observed legs departing less than 6 minutes late, Infrabel's own threshold: 90.9%.
