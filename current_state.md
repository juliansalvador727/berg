# Current state

Written 2026-07-17. A snapshot of where the data rebuild stands, what is broken, what was
fixed, and exactly how to resume. If this file disagrees with `plan.md`, trust this file and
`docs/data-notes.md` — plan.md's numbers are estimates, these are measurements.

## TL;DR

Four data bugs were found and fixed in code (all committed, tests green). Two of them produced
*wrong data that looked right*, so the entire archive is being re-ingested to flush them out.
That rebuild is **19 months of 102 done** and **currently stopped** — it died at 00:13 when the
laptop closed. Nothing is corrupt; it just needs restarting. See [Resume](#resume).

## Where the rebuild is

Chunk 1 (2018-01 → 2020-12) ran to **2019-07 complete**, then died mid-way through 2019-08.

| Chunk | Range | Status |
|---|---|---|
| 1 | 2018-01 → 2020-12 | 2018-01 → 2019-07 rebuilt (19 mo). 2019-08 → 2020-12 pending. |
| 2 | 2021-01 → 2023-12 | Not started (2021-01 rebuilt separately during debugging) |
| 3 | 2024-01 → 2026-06 | Not started (2025-09 rebuilt separately during debugging) |

Verified gains so far: 2018-05..12 **+739,934 legs (+2.39%)**; 2019-01..04 **+242,778 (+1.55%)**.
2019-03 gained +2.63% because the dim fix compounded on top of the one-clock fix.

Disk held flat the whole way: `berg.duckdb` steady at 21 GB, 778 G free. Space is not the
binding constraint it was feared to be, but the per-month raw scratch (~11-13 GB) still must be
cleared between chunks.

### Nothing is corrupt — why

`build_month.py` completes `fct_legs` for the whole month **before** exporting any day file. A
kill mid-month therefore leaves a mix of rebuilt days and old days, each one internally complete
and valid. There is no such thing as a half-written day here.

The R2 bucket is in that same mixed state right now: 2018-01 → 2019-07 carry all four fixes,
2019-08 onward are still the old data. Every day it serves is valid; some are just stale. The
manifest's per-day leg counts are stale too — it is only regenerated at the end of a backfill
run.

## Resume

**One cleanup step is mandatory first.** `data/raw/istdaten/2019-08/` holds 31 CSVs and looks
complete, but the process died during that month, so the last file may be truncated — and
`raw_zip` trusts any existing month dir as-is ("delete it to force a re-download"), so it would
happily build on a truncated file. Delete it:

```bash
rm -rf /home/julian/berg/data/raw/istdaten/2019-08 /home/julian/berg/data/raw/zips/19_08.zip
```

Then:

```bash
cd /home/julian/berg/pipeline
set -a; . ../.env; set +a
nohup .venv/bin/python scripts/backfill.py 2019-08 2020-12 --force > ../data/chunk1.log 2>&1 &
```

Then chunk 2 (`2021-01 2023-12 --force`) and chunk 3 (`2024-01 2026-06 --force`), clearing raw
scratch between each.

### Two traps in that command

- **Keep `--force`.** Without it nothing rebuilds. `month_looks_done` only asks "does this month
  look complete?" — and the old pre-fix months *are* complete. Right day count, right file
  sizes, wrong numbers inside. It cannot distinguish "rebuilt with fixes" from "old but fine",
  so without `--force` it silently skips everything.
- **Don't restart from 2018-01.** `--force` rebuilds unconditionally, so it would redo 19 months
  of finished work.

### Cleanup between chunks

```bash
rm -rf data/raw/istdaten/* data/raw/zips/*
# then, in duckdb: DELETE FROM stg_istdaten; CHECKPOINT;
```

Leave the three M0 CSVs alone (`2018-01-01istdaten.csv`, `2023-06-01_istdaten.csv`,
`2026-06-03_IstDaten.csv`) — their filenames are referenced in `archive.py`, tests, and docs as
examples of ZIP member-name shapes.

The backfill lock is `flock`-based, so a stale `data/backfill.lock` from the dead run blocks
nothing.

## After all three chunks

1. **Geometry** (~3 min, not hours):
   ```bash
   cd /home/julian/berg/geometry && ./.venv/bin/python -m berg_geometry.build \
     --pbf /home/julian/berg/data/raw/switzerland-latest.osm.pbf \
     --db /home/julian/berg/data/berg.duckdb \
     --dim /home/julian/berg/data/dim_station.parquet \
     --out /home/julian/berg/data/publish/static/routes.bin
   ```
2. `dim_route` validation + `train_types` materialize
3. `scripts/sync_publish.py` (needs `.env` sourced) — regenerates the manifest and pushes.

## The four bugs

All four were found the same way: by chasing a number that had no explanation. Each is
committed with tests.

### 1. `fd2a55e` — deflate64, and partial months published as successes

**What broke:** 2025-01's ZIP mixes two compression formats *inside one file* — days 01-27 are
deflate, days 28-31 are deflate64, which Python's stdlib cannot read. So the month ingested 27
days cleanly and blew up on the last four.

**What it caused:** far worse than four missing days. 64 published days were **garbage recorded
as successes** — 2023-09-02..29, 2025-01-28..31 and 2023-03-01 each held ~24 rows instead of
~130,000, and the manifest listed them as fine. The archive was never at fault; every day was
present at full size. The month ingest died partway and publish shipped the fragment.

Two things made it permanent:
- No per-day floor, so a 24-row day looked like a day.
- `month_looks_done` skipped any month with ≥25 parquet files — so 2023-09's 30 broken files
  meant every re-run skipped it forever.

**Fix:** `zipfile-deflate64` dependency; `MIN_LEGS_PER_DAY = 10_000` + `validate_month_days`
(raises `RuntimeError`, deliberately **not** `sys.exit` — `SystemExit` escapes backfill's
`except Exception` and would kill the whole range); `month_looks_done` now requires the
census-expected day count *and* no undersized file. `expected_absent_days()` exempts the
census's 29 genuine holes.

### 2. `deb10b0` — absurd duration amplified by the split rule

**What broke:** legs longer than an hour get chopped into hourly sub-legs. Nothing bounded the
input.

**What it caused:** on 2025-09-10, one source row dated **1899** produced a 126-year leg, which
the split rule turned into **1,101,851 sub-legs from a single row**. Only ~10 landed in an
export window, so roughly 5 duplicate legs actually shipped — but the blast radius was
unbounded by luck, not design.

**Fix:** quarantine `dur > 24h` as `absurd_duration` **before** the split, since past that point
`dur` is an amplification factor. Only 190 legs in 417M exceed it. The test proves it: without
the guard the fixture writes `legs_written: 1043138`.

### 3. `53a4bef` — absence from a GTFS snapshot is not a station closure

**What broke:** the station dimension started a new validity segment whenever a station was
*absent* from a weekly snapshot. That reads as correct and is wrong: the feed drops stations for
weeks and returns them byte-identical.

**What it caused:** holes in validity ranges. Every leg landing in a hole was quarantined as
`unmatched_station` — which looks exactly like the foreign-station noise the pipeline
legitimately expects, so nothing ever complained. Assens (8501172) had five segments at one
lon/lat including a 63-day hole in 2025, and trains called there throughout. **3,351,481
unmatched legs; 13,663 stations had multiple segments with identical attributes.**

**Fix:** segment on attribute change and first appearance only, never on absence. A leg *is* the
evidence the station was serving. Rebuilt dim: 321,621 → **124,508 rows**, same 42,874 stations,
0 overlaps.

**Recovers 1,708,835 legs** — not the 1,985,130 a naive count gives; 277,552 of those are
foreign and merely move to the `outside_ch` quarantine.

### 4. `b7a17ca` — one clock per leg (the worst one)

**What broke:** a leg's duration was computed by picking each end's *best available* time
independently. When a stop had a measured departure but the next stop had only a scheduled
arrival, the code subtracted a scheduled time from an actual one — mixing two clocks.

**What it caused:** wrong durations that look completely plausible, plus millions of negative
ones thrown away. Evidence from 2019-03, split by (departure measured, arrival measured):

| dep / arr measured | legs | negative | rate |
|---|---|---|---|
| False / False | 709,048 | 2 | 0.0% |
| False / True | 58,185 | 11 | 0.02% |
| **True / False** | **138,891** | **46,700** | **33.62%** |
| True / True | 3,176,377 | 42,612 | 1.34% |

Legs sharing a basis are ~0% negative; mixed legs are 33.6% negative. The negatives were the
visible tip — **~6M published legs (1.44%) carry a two-clock duration** and simply came out
wrong-but-positive.

**Fix:** both ends come from the same basis, or the leg falls back to the timetable entirely
(flagged `FLAG_SCHEDULED_FALLBACK`). Departure delay is computed separately, since it needs only
one stop's two times and survives the fallback.

**Verified on 2019-03:** `negative_duration` 89,325 → **42,616** (predicted 42,612); legs
3,913,180 → **3,954,722** (+41,542, +1.06%).

### Why the last two are the dangerous kind

Bugs 1 and 2 announce themselves — a crash, a 1.2 KB file. Bugs 3 and 4 produced data that
passed every integrity check: right row counts, plausible durations, no errors. They were only
found by asking why a quarantine bucket was the size it was. That is the argument for
quarantine-with-reason over silent drops; without the reason codes neither would have surfaced.

## Quarantine totals (pre-rebuild)

9,920,009 legs = **2.32%** of candidates.

| Reason | Legs | Share |
|---|---|---|
| `negative_duration` | 5,305,810 | 53.5% |
| `unmatched_station` | 3,351,481 | 33.8% |
| `zero_duration` | 1,229,278 | 12.4% |
| `missing_time` | 33,419 | 0.3% |
| `absurd_duration` | 21 | ~0% |

The rebuild should collapse the first two substantially.

## Known gaps — not yet audited

Disclosed rather than resolved:

- **`missing_time`** (33,419 legs) — never investigated.
- **`zero_duration`** (~1.2M) — analysed but undecided. 99.8% are CH-CH and only 1,844 are
  same-station; median station distance is 966 m and 89.4% are under 2 km, so these are real
  short hops whose booked times round to the same minute (only 27% land on-the-minute). They
  are currently *dropped*. A duration floor may be more honest than a drop. The one-clock fix
  *increases* this bucket by ~3k/month, since short booked hops now round to zero rather than
  going negative.
- **The UI has never been opened in a browser.** The delay-colour toggle and legend
  (`100603c`) are unverified — no browser tool available. `npm run dev` in `web/` is outstanding.

## Geometry risk: LOW (audited 2026-07-16)

Checked before committing to the rebuild, because new station pairs need new routes.

Only **78** new pairs fall inside `CH_BBOX`. The alarming raw figure of 707 new pairs is mostly
foreign — Berlin Hbf, Toulon, Pavia — and dies at the `outside_ch` filter anyway. Of the 94
stations involved, 85 are known and 9 are new; **9/9 snap** to the rail graph (p50 6.9 m, max
142.8 m against a 300 m ceiling).

The job is ~3 minutes, has a detour guard (`DETOUR_RATIO = 4.0` **and**
`DETOUR_EXCESS_M = 10_000` — both must hold) and a straight-line fallback, and writes
`routes.report.json`. Current: 4,614 pairs, 79 fallbacks (76 unsnappable Italian, 3
`absurd_detour`), ratio p50 1.07, p99 2.355.

## Architecture recap

Historical playback map of every measured Swiss train movement, replayed in the browser. **No
backend in the serving path** — Parquet on Cloudflare R2, DuckDB WASM in the browser, deck.gl +
MapLibre, deployed on Cloudflare Pages.

The design driver is cost: R2's free tier (10 GB, zero egress) forces an ~8-byte-per-leg wire
schema with geometry factored out into a shared `routes.bin` (413 KB).

Layout: `pipeline/` (Dagster + DuckDB), `geometry/` (one-off OSM rail-graph job, pyosmium +
scipy), `web/` (Vite + TS), `infra/` (R2/CORS/CI), `docs/data-notes.md` (verified data facts).
`data/publish/` is the gitignored local staging mirror.

M0–M4 are all committed. `berg_pipeline/ingest.py` holds the contract SQL.

## R2 bucket

Public base `https://pub-40f06e4404c049578963083898f4ab57.r2.dev` (`web/.env.local`; prod
`https://data.berg.ch`). Credentials live in `/home/julian/berg/.env` — **repo root, not
`pipeline/`** — and must never be committed.

As last fully synced: legs 3,104 + journeys 3,104 (parity), static 5, **5.09 GB of the 10 GB
tier**, manifest 2018-01-01 → 2026-07-01, `missing_days: 0`. CORS
(`Access-Control-Allow-Origin: https://berg.ch`) and range requests (206) verified correct.

Journeys cost 827 KB/day — the sidecar is as big as legs itself, because `trip_id` is a fat
VARCHAR while a legs row is a packed 8 bytes.

## Data facts worth remembering

- Archive: monthly ZIPs, ~1.27 TB total, **3,074 usable days**, 29 genuine census holes.
- Healthy day ≈ **110k–150k legs**; 2018 is the light end. **Never trust a manifest day entry as
  proof of a good day — check the leg count.**
- `trip_id` has two formats: **75.8%** `85:...`, **23.7%** `sjyid...`. The split is by era —
  ~100% `85:` for 2018-2023, then 35% (2024), 18% (2025), 9% (2026). Any train-number search
  needs a two-format extractor. (An earlier claim of "98.6% numeric" was measured *inside* the
  `85:` filter and was biased.)
- `fct_legs.line` is only populated for months ingested after the `m3: LINIEN_TEXT` commit —
  `_ADDED_COLUMNS` ALTERs it in and old rows keep NULL. NULL here means "staged before this
  column existed", not "absent from source". The full rebuild fills it everywhere.
- Coverage questions can be answered cheaply with HTTP range requests against ZIP central
  directories (`remote_zip.py`) — no need to download 15 GB to ask what days exist.

## Ideas not yet built

- **Cancellations layer** — `FAELLT_AUS_TF` is already ingested and counted in `_COUNT_SQL`,
  then filtered out at `ingest.py:242`. The data is there.
- **`umlauf_id`** — rolling stock working numbers; would let you follow a physical train.
- **Train-number search** — needs the two-format `trip_id` extractor above.

## Conventions

- **Commits must never mention or be attributed to any AI assistant.**
- `plan.md` is gitignored deliberately and its prose must not be copied into tracked files.
- Redact secrets in any pasted output: `sed -E 's/[A-Za-z0-9_-]{25,}/<redacted>/g'`.
