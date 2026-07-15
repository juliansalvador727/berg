# Ist-Daten: what the data actually does

Measured against one full service day (2026-06-03, a Wednesday) during the M0 spike. Numbers are
one day, so treat them as an order of magnitude, not gospel — but every claim here was verified
against real data rather than documentation.

Reproduce with `pipeline/scripts/m0_export.py`.

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

## Schema

Delimiter `;`. Columns:

```
BETRIEBSTAG FAHRT_BEZEICHNER BETREIBER_ID BETREIBER_ABK BETREIBER_NAME PRODUKT_ID LINIEN_ID
LINIEN_TEXT UMLAUF_ID VERKEHRSMITTEL_TEXT ZUSATZFAHRT_TF FAELLT_AUS_TF BPUIC HALTESTELLEN_NAME
ANKUNFTSZEIT AN_PROGNOSE AN_PROGNOSE_STATUS ABFAHRTSZEIT AB_PROGNOSE AB_PROGNOSE_STATUS
DURCHFAHRT_TF SLOID
```

**`SLOID` is its own column** — it is not stuffed into `BPUIC`. `BPUIC` stays a clean integer.

### Measured status is `REAL`, not `GESCHAETZT`

This is the one that matters most, and it is the opposite of what v1 lore says.

`AB_PROGNOSE_STATUS` for `PRODUKT_ID='Zug'`, one day:

| Status       |       n | note                      |
| ------------ | ------: | ------------------------- |
| `REAL`       | 141,952 | measured — **use this**   |
| (null)       |  15,655 | no realtime               |
| `PROGNOSE`   |  14,239 | forecast — never render   |
| `UNBEKANNT`  |  12,157 | unknown                   |
| `GESCHAETZT` |   **0** | never appears for trains  |

`GESCHAETZT` ("estimated") does occur — 47,624 times across the whole feed — but only for buses.
For trains in v2 the measured status is `REAL`. About **87% of legs have a measured departure**.

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

## Volume (one day, `PRODUKT_ID='Zug'`)

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

### Foreign stops are ~10%

By BPUIC country prefix: CH (85) 165,004 · DE (80) 9,086 · FR (87) 4,040 · AT (81) 3,694 ·
IT (83) 2,142. Clipping to the CH bounding box drops roughly a tenth of stop events.

## Data quality

- **710 legs/day have a negative duration** (0.44%) — the next stop's arrival is recorded before
  this stop's departure. **625 of them are CH→CH**, so this is not a timezone artifact; it is
  genuine source error. Quarantine and count them; do not clamp silently.
- **284 legs/day exceed 3600 s**, so the split rule is load-bearing. Every one is a NightJet or
  international run (worst: Feldkirch→Siebnen-Wangen at 18,511 s ≈ 5.1 h).

## Storage: the budget is not tight

Measured on one real day encoded to the 8-byte wire schema, zstd:

| Row group | Size/day | Bytes/leg |
| --------- | -------: | --------: |
| 8,192     |  1.03 MB |  **6.37** |
| 60,000    |  0.93 MB |      5.72 |
| 122,880   |  0.90 MB |      5.53 |

**The M1 gate passes**: 6.37 B/leg against an 8-byte budget and a 10-byte re-plan threshold.

Extrapolated over real coverage (2018-01 → 2026-06, ~3,100 days): **~502M legs, ~3.2 GB** against
the 10 GB free tier. The plan assumed 800M legs and 5–7 GB with "thin" headroom and dropping the
oldest years as an escape hatch. **Neither is necessary** — there is ~6.8 GB spare, and the
oldest years are cheap because they don't exist.

Also validated: **9,440 legs depart in the hour before 08:00**, matching the planned ~8–10k rows
per one-hour row group almost exactly.

## Stations: use GTFS, not DIDOK

The CKAN API returns 403 and `atlas.api.opentransportdata.swiss` does not resolve. But the same
archive publishes **GTFS back to 2016, weekly** (`timetable_gtfs.php`), and `stops.txt` is 2.6 MB
zipped inside a ~130 MB archive — trivially extractable by range request. Weekly snapshots map
naturally onto the SCD2 validity ranges `dim_station` needs.

**The BPUIC is the numeric prefix of `stop_id`.** Both `8509404:0:1` (platform) and
`Parent8509404` carry 8509404. Many stations — Buchs SG among them — exist *only* as platform
rows, so matching plain 7-digit ids takes the join from **47% to 90%**. Platform coordinates
differ by ~100 m; averaging them to a station centroid is fine at map zoom.

Of the 10% that miss, essentially all are distant foreign stations (Düsseldorf Hbf, St. Pölten
Hbf) that the CH bounding box drops anyway. **Only 3 of 2,204 Swiss stops fail to match.**
