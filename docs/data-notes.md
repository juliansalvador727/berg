# Ist-Daten: what the data actually does

Measured against one full service day (2026-06-03, a Wednesday) during the M0 spike, and
extended at M1 by a census of **every month 2018-01 → 2026-06**, header samples across both URL
series, and full-day probes of the v1 era (2018-01, 2018-05, 2019-07, 2023-06, 2025-08). Every
claim here was verified against real data rather than documentation.

Reproduce with `pipeline/scripts/m0_export.py` (one day → browser binary),
`pipeline/scripts/probe_era.py` (one day → schema/enum/volume report),
`pipeline/scripts/find_status_switch.py` (enum boundary),
`pipeline/scripts/archive_census.py` (coverage map → `docs/archive-census.json`),
`pipeline/scripts/build_dim_station.py` (the SCD2 station dimension), and
`pipeline/scripts/build_month.py` (one month end to end through the Dagster assets).

## The archive

Range requests work (`accept-ranges: bytes`), and a ZIP keeps its index at the end. So you can
list a monthly archive's members and pull **one day (47 MB) out of a 1.4 GB month** without
downloading the month. `pipeline/scripts/` has a minimal implementation.

One member per service day, `YYYY-MM-DD_IstDaten.csv`, ~630 MB raw / ~50 MB zipped.

### Four naming eras — there is no single URL pattern

| Period            | Path                                              |
| ----------------- | ------------------------------------------------- |
| 2016 + 2017       | `istdaten/2016+2017/16+17-unvollstaendig.zip`     |
| 2018-01 → 2021-05 | `istdaten/YYYY/YY_MM.zip`                         |
| 2021-06 → now     | `istdaten/YYYY/ist-daten-YYYY-MM.zip`             |
| 2025-07 → now     | `istdaten/YYYY/ist-daten-v2-YYYY-MM.zip` (v2)     |

### 2016–2017 is not usable

That single ZIP is **166 MB for twenty-four months**, against 397 MB for January 2018 alone. The
filename says it: *unvollständig*, incomplete. **Real coverage starts 2018-01.**

### v1 is not retired

Both v1 and v2 are published in parallel, through 2026-06 at least. There is no forced migration
yet — but v2 is the one to build on.

### Member paths inside the ZIPs drift too — never construct one

The naming eras above are only the *file* names. Member paths have taken at least six shapes:

```
jan18/2018-01-01istdaten.csv          mai18/2018-05-06istdaten.csv
18_10/2018-10-01istdaten.csv          19_7/2019-07-01istdaten.csv
20_04/2020-04-01_istdaten.csv         2022-01-01_istdaten.csv          (bare, no folder)
ist-daten-2023-04/2023-04-01_istdaten.csv                2026-06-03_IstDaten.csv
```

Folder prefix, underscore before `istdaten`, and capitalisation all vary; the folder name does
not reliably match the month's own naming era. **The only stable thing is an ISO date in the
basename.** Always list the ZIP and match on that — `berg_pipeline.archive.day_members()`.

### Three months carry `__MACOSX` junk that looks like data

2023-03, 2023-05, 2024-05, 2024-10 and 2024-11 were zipped on a Mac and ship AppleDouble
resource forks: `__MACOSX/._2023-03-01_istdaten.csv`, ~300 bytes, **with a `.csv` extension**.
2023-03 has 61 "CSV" members for a 31-day month. A naive glob ingests 30 empty days. Filter
`__MACOSX/` and basenames starting with `._`.

### The archive has holes: 29 missing days

`archive_census.py` reads each month's central directory (no downloads) and finds **3,074 usable
days of an expected 3,103**, totalling **1.27 TB** raw — not the 1.8 TB the plan assumed. Full
map in `docs/archive-census.json`. Missing days come in two flavours: **absent** (no member at
all, 12 days) and **stub** (a member of ~20 KB, 17 days). Both must be treated as no-data:

| Gap                       | Days | Kind   |
| ------------------------- | ---: | ------ |
| **2019-07-01 .. 07-16**   | **16** | stub |
| 2021-07-23 .. 07-25       |    3 | absent |
| 2018-05-24, 2019-03-02, 2021-07-14, 2021-07-19, 2021-10-15, 2022-07-24, 2022-08-08, 2022-08-17, 2022-09-04 | 9 | absent |
| 2022-11-09                |    1 | stub   |

Half of July 2019 is simply not there. **Consequences:** the backfill must not treat a missing
day as failure; `manifest.json` needs an explicit missing-days list; and the frontend scrub bar
has to skip gaps rather than show a frozen map for sixteen days.

Also: **the current month is not published** (2026-07 404s mid-month), so coverage ends at the
last complete month.

## Schema

Delimiter `;`. Columns:

```
BETRIEBSTAG FAHRT_BEZEICHNER BETREIBER_ID BETREIBER_ABK BETREIBER_NAME PRODUKT_ID LINIEN_ID
LINIEN_TEXT UMLAUF_ID VERKEHRSMITTEL_TEXT ZUSATZFAHRT_TF FAELLT_AUS_TF BPUIC HALTESTELLEN_NAME
ANKUNFTSZEIT AN_PROGNOSE AN_PROGNOSE_STATUS ABFAHRTSZEIT AB_PROGNOSE AB_PROGNOSE_STATUS
DURCHFAHRT_TF SLOID
```

**`SLOID` is its own column** — it is not stuffed into `BPUIC`. `BPUIC` stays a clean integer.

### "v1 vs v2" is not a schema distinction — the schema is date-driven

The obvious assumption (v1 = 21 columns, v2 = 22 with `SLOID`) is **wrong**, and it fails in both
directions. Headers of the first day of each month, both URL series:

| Month   | `ist-daten-YYYY-MM.zip` ("v1") | `ist-daten-v2-YYYY-MM.zip` |
| ------- | ------------------------------ | -------------------------- |
| 2025-07 | 21, no SLOID                   | **21, no SLOID**           |
| 2025-10 | 21, no SLOID                   | 21, no SLOID               |
| 2025-11 | **22, SLOID**                  | **22, SLOID**              |
| 2026-06 | 22, SLOID                      | 22, SLOID                  |

**`SLOID` appears in 2025-11, in both series simultaneously** — four months *after* v2 launched.
The two series have had **identical headers in every month checked** (2018-01 → 2026-06); the
first 21 columns are byte-identical in name and order throughout the archive.

So there is no v1 schema and no v2 schema, only a **date**: 21 columns before 2025-11, 22 after.
And since `SLOID` is a column we don't use, the "one normalizing view per era" the plan calls for
is really just **select the columns you need by name** — that works for every month of both
series, with no era branch at all.

### The two series do differ — but barely, where it counts

Identical headers, different contents: v2 is ~9% larger on disk. For 2025-08-03, all rows:

| Series | Train rows | `REAL` | Products                                       |
| ------ | ---------: | -----: | ---------------------------------------------- |
| v1     |    146,757 | 120,603 | Bus, Tram, Zug, Zahnradbahn, Schiff            |
| v2     |    162,793 | 121,645 | …same **+ Metro** (3,265)                      |

v2 carries **+10.9% train stop events but only +0.9% more measured ones** — the extra ~16k rows
are almost all `PROGNOSE`/`UNBEKANNT`/null, i.e. forecasts we never render. v2 also reports a
`Metro` product that v1 omits entirely.

**Why this matters:** the plan uses v1 for 2018-01 → 2025-06 and v2 from 2025-07, so a series
switch sits in the middle of the dataset. Measured legs differ by ~1% across it, so **the seam
will not show as a step in the map**. Good news, but check it again if the fallback-to-scheduled
path ever renders — those rows differ by 11%.

### Measured status is `REAL` — but the boundary is a DATE, not the file version

This is the one that matters most, and the M1 probe overturned the M0 conclusion.

M0 (v2 only) found measured = `REAL` and no `GESCHAETZT` at all, and concluded that `GESCHAETZT`
was v1 lore. **Half right.** `GESCHAETZT` *is* the old value, but it died in **May 2018**, seven
years before v2 existed:

| Day          | Format    | `AB_PROGNOSE_STATUS` for trains        |
| ------------ | --------- | -------------------------------------- |
| 2018-05-04   | v1 (21 c) | `GESCHAETZT`                           |
| **2018-05-06** | v1      | **MIXED** — `GESCHAETZT` 63,275 · `REAL` 15 |
| 2018-05-08   | v1        | `REAL`                                 |
| 2023-06-01   | **v1**    | **`REAL`** ← still v1, already REAL    |
| 2026-06-03   | v2        | `REAL`                                 |

So the enum boundary is **2018-05-07**, and the format boundary is **2025-07**. They are
unrelated, and 2018-05-06 mixes both values in one file. Only ~4 months of 102 are `GESCHAETZT`
— not the 83 the plan feared.

**The rule: don't branch on era. Match the set.**

```sql
WHERE upper(PRODUKT_ID) = 'ZUG' AND AB_PROGNOSE_STATUS IN ('REAL', 'GESCHAETZT')
```

This is era-free, survives the mixed day, and is safe because the two are mutually exclusive per
day everywhere else. `GESCHAETZT` does still appear post-2018 — but only for **buses**, which
the `Zug` filter already removed. (`berg_pipeline.constants.MEASURED_STATUSES`.)

Full v1 day, 2018-01-01: `GESCHAETZT` 94,196 · `PROGNOSE` 21,067 · `UNBEKANNT` 16,101, and
**no nulls** — unlike v2, which nulls the status when there is no realtime.

`AB_PROGNOSE_STATUS` for `PRODUKT_ID='Zug'`, one v2 day:

| Status       |       n | note                      |
| ------------ | ------: | ------------------------- |
| `REAL`       | 141,952 | measured — **use this**   |
| (null)       |  15,655 | no realtime               |
| `PROGNOSE`   |  14,239 | forecast — never render   |
| `UNBEKANNT`  |  12,157 | unknown                   |
| `GESCHAETZT` |   **0** | never appears for trains  |

`GESCHAETZT` ("estimated") does occur — 47,624 times across the whole feed — but only for buses.
For trains in v2 the measured status is `REAL`. About **87% of legs have a measured departure**.

### Timestamp formats are cleanly split by column, in both eras

M0 recorded "two formats coexist in one file", which is true but understates the regularity.
Across 2018-01, 2019-07, 2023-06 and 2026-06, every non-null value obeys:

| Column                      | Format             |
| --------------------------- | ------------------ |
| `ABFAHRTSZEIT`/`ANKUNFTSZEIT` (scheduled) | `DD.MM.YYYY HH:MM`    |
| `AB_PROGNOSE`/`AN_PROGNOSE` (actual)      | `DD.MM.YYYY HH:MM:SS` |

Zero unparsed values in any probe. Keep using `try_strptime` with the format list — it costs
nothing and the invariant is not contractual.

### There is no stop-sequence column

Nothing gives you the order of stops within a run. You must order by **scheduled** time
(`ABFAHRTSZEIT`), not actual: a delayed train's actual times go non-monotonic and scramble the
sequence.

### Other traps

- **`PRODUKT_ID` has both `Bus` and `BUS`** (2,094,802 vs 70,787). Case drift is real — always
  `upper()`. `Zug` happens to be consistent today; do not rely on that.
- **Two timestamp formats in the same file**: `DD.MM.YYYY HH:MM` (scheduled) and
  `DD.MM.YYYY HH:MM:SS` (actual). `try_strptime` with a format list handles both.
- **Autodetect is actively dangerous.** DuckDB's `read_csv` silently typed `BETRIEBSTAG` as
  `DATE` on this file. Read everything with `all_varchar=true` and parse explicitly, or a schema
  change three years into the backfill becomes a silent cast instead of an error.

### v2 launched mid-month — its first month is short, and silently so

**v2 started publishing on 2025-07-13.** So for the seam month the two series disagree, and
only there:

| month   | v1 days | v2 days |
| ------- | ------: | ------: |
| 2025-07 |  **31** |  **19** |
| 2025-08 |      31 |      31 |
| 2025-09 → 2026-06 | full | full |

Probed against every v2-era month's central directory. `2025-07` is the **only** month where
they differ — and v1 is complete for all of them, which is the practical proof that v1 is not
retired.

Selecting the series on *"does v2 exist yet"* (`>= 2025-07`) therefore drops **12 real days**
and still reports the month a success — the worst kind of bug, because a short month is
indistinguishable from a good one downstream. Switch at v2's first **complete** month
(`V2_FIRST_FULL_MONTH = (2025, 8)`) and take v1 for the seam.

Two lessons, both already written down elsewhere in this file and both ignored by that line:
**the naming does not predict the contents** (a v2 URL existing says nothing about what is in
it), and **a month that comes up short must fail loudly** — `raw_zip` now cross-checks the
extracted day count against this census and refuses to stage a short month.

### Encoding is per FILE, not per month — one month mixes UTF-8 and latin-1

`2018-11` killed the backfill on `Invalid unicode ... This file is not utf-8 encoded`. It is
not a per-month property:

| file              | utf-8 | latin-1 |
| ----------------- | ----- | ------- |
| `2018-11-01.csv`  | OK, 1,062,898 rows | **rejected** |
| `2018-11-02.csv`  | **rejected** | OK, 1,188,574 rows |

Consecutive days, opposite encodings. So there is no single `encoding=` to hand `read_csv`,
and **latin-1 is not a catch-all** — DuckDB validates it and rejects the UTF-8 file, so
"just always read latin-1" fails half the month too (and would silently mojibake the rest).

Detect per file and `UNION ALL` one `read_csv` per encoding group. Detection is cheap
(~0.3 s/0.5 GB): a latin-1 file exits at its first bad byte, and only genuine UTF-8 files are
read through.

The irony: **every offending byte is in a column this pipeline discards.** The bad bytes are
`0xFC`/`0xE4`/`0xF6` — `Baden-Württemberg` (BETREIBER_NAME), `Möhlin`/`Bossière`
(HALTESTELLEN_NAME). Operator comes from `BETREIBER_ABK` and station names from
`dim_station`, so a month died over umlauts it was going to throw away. There are no
`0x80–0x9F` bytes, so the payload is plain ISO-8859-1, not CP1252.

### Rows can be truncated — one short line failed an entire month

`2024-10-26.csv` **ends mid-row**: its last line (1,606,824) carries 16 of 21 columns, cut off
after `AN_PROGNOSE`. Exactly one row in 1.6M — and it is a `BUS`, which the `PRODUKT_ID='Zug'`
filter discards anyway. DuckDB's strict mode still failed the whole month over it, and the
backfill's per-month `except` skipped 2024-10 silently.

Read with **`null_padding=true`**. Truncated rows then pad to `NULL`, which degrades the right
way: a bus is dropped by the `Zug` filter, and a truncated *train* row gets a NULL
`AB_PROGNOSE_STATUS`, so it fails `MEASURED_STATUSES` and lands in scheduled-fallback or
quarantine instead of killing 31 days.

Do **not** use `ignore_errors=true` for this. It silently discards rows, which is how you lose
train data and never find out. Padding keeps the row and lets the existing filters judge it.

The general lesson again: **the archive's shape does not predict its contents.** A file that is
1.6M rows of clean CSV can still stop mid-sentence on the last line.

## Volume (one v2 day, `PRODUKT_ID='Zug'`)

| Thing                       | Measured | Plan assumed |
| --------------------------- | -------: | -----------: |
| Runs                        |   16,252 |         ~15k |
| Stop events                 |  184,003 |            — |
| Stops per run (mean/median) | 11.3 / 10 |         ~15 |
| **Legs**                    | **162,913** | 200–225k |
| Distinct station pairs      |    5,854 |         ~20k |
| Cancelled stop events       | 5,080 (2.8%) |          — |
| Peak concurrent trains      |    ~555 @ 10:00 | 600–1,000 |

Legs run ~27% below plan, and distinct station pairs are **3.4× fewer** than the ~20k the route
job was sized for — though pairs will accumulate over a decade, so don't size `routes.bin` off
one day.

### The feed grows ~25% across the archive

Train stop events on the first of the month, one day per era:

| Day          | Stop events | Runs   | Legs ≈ (events − runs) |
| ------------ | ----------: | -----: | ---------------------: |
| 2018-01-01 (New Year, quiet) | 131,364 | 12,311 | ~119k |
| 2023-06-01   |     162,220 | 15,319 |                 ~147k |
| 2026-06-03   |     184,003 | 16,252 |                 ~168k |

So legs/day is **not** constant at M0's 163k — early years are lighter. Taking ~150k/day mean
across 3,074 usable days: **~460M legs ≈ 2.9 GB** at 6.37 B/leg. That is *below* M0's 3.2 GB
projection (which assumed today's volume for every day), and comfortably inside the 10 GB tier.

Note also 2018's raw days are ~200 MB against 2026's ~600 MB — the 1.27 TB total is
front-loaded lighter, which helps the backfill.

### Foreign stops are ~10%

By BPUIC country prefix: CH (85) 165,004 · DE (80) 9,086 · FR (87) 4,040 · AT (81) 3,694 ·
IT (83) 2,142. Clipping to the CH bounding box drops roughly a tenth of stop events.

## Data quality

- **710 legs/day have a negative duration** (0.44%) — the next stop's arrival is recorded before
  this stop's departure. **625 of them are CH→CH**, so this is not a timezone artifact; it is
  genuine source error. Quarantine and count them; do not clamp silently.
- **The 2018 feed is ~6× worse: ~3,100 negative-duration legs/day (2.4%)**, measured over all of
  2018-05 at month scale. They are evenly spread across days and identical on both sides of the
  GESCHAETZT→REAL switch (76.2% vs 73.8% measured), so this is feed quality improving over the
  years, not an enum artifact. Do not tune quarantine alarms to the 2026 rate.
- **284 legs/day exceed 3600 s**, so the split rule is load-bearing. Every one is a NightJet or
  international run (worst: Feldkirch→Siebnen-Wangen at 18,511 s ≈ 5.1 h).

### Month scale (2018-05, the hardest month, through the real pipeline)

First full month through the Dagster assets (`raw_zip → stg_istdaten → fct_legs →
legs_parquet`), chosen because it contains the enum switch (05-07), the mixed day (05-06), and
a missing day (05-24):

| Thing                       | 2018-05 measured |
| --------------------------- | ---------------: |
| Legs                        | 3,803,102 (126,770/day over 30 service days) |
| Measured (vs fallback)      | 74.3% — 2018 realtime coverage, not a bug (2026 is ~87%) |
| Distinct station pairs      | 4,614 in the whole month (2026: 5,854 in one day) |
| Split sub-legs              | 3,954 (~132/day; fewer night trains than 2026's 284/day) |
| Max stops in one run        | 48 — `FAHRT_BEZEICHNER` is sane as a per-day trip id |
| Delay p50 / p99             | 60 s / 435 s |
| Quarantined                 | negative_duration 93,065 · zero_duration 13,512 · unmatched_station 155 · missing_time 52 |
| Day files                   | 31 files, 23.7 MB total, **6.23 bytes/leg** — the gate passes at month scale |

Two boundary behaviors worth remembering:

- **Day files are keyed by the DEPARTURE day in UTC**, not the service day — local 00:00–01:59
  departures land in the previous UTC day's file, which is what the client's
  "fetch day N and N−1 near midnight" rule expects.
- **The 05-24 archive hole produces a 311-leg file** (the night tail of 05-23's trains), not a
  missing file — data-faithful, and the scrub bar treatment (M4) must handle "nearly empty",
  not just "absent".

## Storage: the budget is not tight

Measured on one real day encoded to the 8-byte wire schema, zstd:

| Row group | Size/day | Bytes/leg |
| --------- | -------: | --------: |
| 8,192     |  1.03 MB |  **6.37** |
| 60,000    |  0.93 MB |      5.72 |
| 122,880   |  0.90 MB |      5.53 |

**The M1 gate passes**: 6.37 B/leg against an 8-byte budget and a 10-byte re-plan threshold.

Extrapolated over real coverage (2018-01 → 2026-06): M0 projected ~502M legs / ~3.2 GB by
assuming today's 163k legs for every one of ~3,100 days. The M1 census refines both inputs —
**3,074 usable days** (29 are missing) and a feed that was ~25% lighter in 2018 — giving
**~460M legs, ~2.9 GB** against the 10 GB free tier.

The plan assumed 800M legs and 5–7 GB with "thin" headroom and dropping the oldest years as an
escape hatch. **Neither is necessary** — there is ~7 GB spare, and the oldest years are cheap
both because 2016–17 don't exist and because 2018 is a lighter feed.

Also validated: **9,440 legs depart in the hour before 08:00**, matching the planned ~8–10k rows
per one-hour row group almost exactly.

## Geometry (M2): the rail graph is one component, so snap per pair, not per station

Measured while routing the 4,614 pairs of 2018-05 against the Geofabrik Switzerland extract
(403k nodes / 410k edges of `railway=rail|narrow_gauge|light_rail`):

- **Nearest-node snapping is wrong at every multi-gauge station.** At Interlaken Ost the
  metre-gauge track is meters closer than the standard-gauge one; snapping to it routed
  West→Ost **187 km via Luzern** (Montreux and Göschenen→Andermatt failed the same way).
- **Snapping per connected component doesn't fix it** — the Swiss networks touch somewhere
  (dual gauge, shared crossing nodes), so it's all one giant component and the wrong-gauge
  snap survives. What works is a **virtual node per station wired to all nearby tracks**
  (≤300 m, cost = distance × 10): Dijkstra minimizes snap + rail + snap per pair and the
  right network falls out of the shortest path. After the fix, Göschenen→Andermatt is 3.8 km
  against the Schöllenenbahn's real 3.7, and ratio_p99 dropped 4.12 → 2.36.
- **The extract is the coverage boundary, not the bbox.** 25 stations inside CH_BBOX have no
  track: the Italian legs of the Simplon/Centovalli lines (Preglia, Varzo, Masera…). And two
  pairs whose real track is missing — Bossonnens→Palézieux (line disused in current OSM) and
  Neuhausen→Rafz (runs through Germany, clipped) — produced 55 km "shortest" paths. The
  detour guard (ratio > 4 AND excess > 10 km → straight line + flag) catches those without
  killing legitimate 6× mountain switchbacks like Chernex→Chamby.
- **routes.bin is 413 KB**, not the ~20 MB planned: 4,614 polylines, 92,970 points after 10 m
  Douglas-Peucker, uint16 quantization (~7 m). 79 routes flagged straight-fallback (1.7%).
  Snap quality: p50 9.4 m, p95 30.9 m. Median rail path is 1.07× the straight line.
- Full report: `data/publish/static/routes.report.json`; format in
  `geometry/src/berg_geometry/binfmt.py` (byte-level contract test on the pipeline side).
- Caveat for the backfill: routes are computed on **today's** OSM. A 2018 service over a
  since-rebuilt alignment renders on the current track. Acceptable; revisit only if a whole
  line's history matters.

## Stations: use GTFS, not DIDOK

The CKAN API returns 403 and `atlas.api.opentransportdata.swiss` does not resolve. But the same
archive publishes **GTFS back to 2016, weekly** (`timetable_gtfs.php`), and `stops.txt` is 2.6 MB
zipped inside a ~130 MB archive — trivially extractable by range request. Weekly snapshots map
naturally onto the SCD2 validity ranges `dim_station` needs.

**The BPUIC is the numeric prefix of `stop_id`.** Both `8509404:0:1` (platform) and
`Parent8509404` carry 8509404. Many stations — Buchs SG among them — exist *only* as platform
rows, so matching plain 7-digit ids takes the join from **47% to 90%**.

Of the 10% that miss, essentially all are distant foreign stations (Düsseldorf Hbf, St. Pölten
Hbf) that the CH bounding box drops anyway. **Only 3 of 2,204 Swiss stops fail to match.**

### The snapshot listing: 722 files, three filename shapes, one trap

`timetable_gtfs.php` is a parseable HTML index — the authority on what exists. Shapes:

```
GTFS_FP2017_2017-01-23.zip          435×   hyphenated date
GTFS_FP2021_2021-02-03_10-01.zip    202×   + publish TIME — a day can publish twice
GTFS_FP2026_20260425.zip             85×   compact date
```

**The trap:** a date regex anchored `$` straight after the date matches the first and third
shapes and **silently drops all 202 of the middle one** — which is every snapshot from 2021 to
2023. It does not error; it just yields a station dimension that is quietly wrong for three
years. `list_snapshots()` therefore *raises* on any listed `.zip` it cannot parse.

`FP<year>` is the **timetable** year, distinct from the publication date, and two FP years
publish concurrently around each December changeover. 722 files → **594 unique publication
days**, median cadence **7 days**; the only gap >21 d is 2016-12-15 → 2017-01-23, before our
range. The listing lags real time by ~2 months (last snapshot 2026-05-03 as of 2026-07).

### One snapshot is truncated

`GTFS_FP2018_2018-08-15.zip` is **67 MB of local-header stream with no central directory at
all** — no EOCD, no ZIP64 record. It fails to open from local disk too, so it is a bad upload,
not a range-request artifact. It is the **only** broken snapshot of 594 (`gtfs-census.json`).

Skipping it is harmless at weekly cadence. Skipping many would not be — it would silently
coarsen the dimension — so the builder fails if >2% are unusable.

### stops.txt: stable schema, four id shapes, no row type in every era

Fields `stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station` are present in
every era (2026 adds `platform_code`, `original_stop_id` — so select by name, never `SELECT *`).
Rows grow 29k (2018) → 98k (2026). Four `stop_id` shapes:

| Shape           | Meaning                       |
| --------------- | ----------------------------- |
| `8501008`       | bare station row              |
| `Parent8501008` | parent station row (`location_type=1`) |
| `8501008:0:1`   | platform                      |
| `8004238P`      | 2018's station row (`location_type=1`) |

**No single kind exists in every era**: 2018 has no `Parent` rows at all (station rows are bare,
or `…P`), and by 2026 **15,465 stations have no bare row**. So the coordinate source must be a
preference chain: bare → parent → `location_type=1` → platform centroid.

### Don't average platforms, and round before comparing

Two rules that only matter because this is SCD2 — either would be invisible in a single
snapshot, and both would otherwise manufacture change events:

- **Coordinate precision drifts**: `46.2102053471586` (2018, 13 dp) vs `46.21021156` (2026,
  8 dp). Raw float comparison emits a change for nearly every station at the boundary. Round to
  **5 dp (~1.1 m)** before diffing.
- **Platform centroid ≠ station row, and the relationship changed.** The centroid matches the
  station row for **86.5% of stations in 2023 but only 11.0% in 2026** (platforms share the
  station's coordinate in 2023 and have distinct ones in 2026). So M0's "average the platforms,
  it's fine at map zoom" — true for one map — would invent a coordinate change for ~89% of
  stations at the era boundary. Station rows agree with each other **100%** of the time
  (bare vs parent, at 5 dp), so the preference chain never jumps.

Averaging is still the fallback for stations that have *only* platform rows (945 in 2018,
including Buchs SG).
