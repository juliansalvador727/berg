# pipeline

Dagster + DuckDB. Turns monthly Ist-Daten archive ZIPs into compact daily leg and journey
Parquet files, a manifest, and static lookup assets for Cloudflare R2.

## Setup

```sh
uv sync
cp ../.env.example ../.env   # fill in R2 credentials
uv run dagster dev -m berg_pipeline.definitions   # http://localhost:3000
```

## Asset graph

```
raw_zip[month] → stg_istdaten[month] → fct_legs[month] → legs_parquet[day] ─┐
                                          ↑               journeys[day] ───┤→ manifest
dim_station ──────────────────────────────┤                                │
dim_route (from geometry/) ───────────────┘                        static assets
```

`dim_station` first — it is the hardest part. It is SCD Type 2 because a 2017 stop event has to
join to the 2017 name and coordinates, and stations get renamed, opened, and closed constantly.

## Current state

The historical backfill is complete through source month 2026-06: 422,103,769 published legs,
3,090 leg days, 3,090 journey days, and 11,502 registered routes. Do not rerun the historical
backfill for ordinary development. See [`../current_state.md`](../current_state.md) for the
validation record and remaining data-quality decisions.

## Building data

The source census totals about 1.27 TB of raw CSV. The historical path is resumable and runs one
month at a time:

```sh
uv run python scripts/build_month.py 2026-06
```

For direct asset work, always tell the Dagster CLI which definitions module to load:

```sh
uv run dagster asset materialize \
  -m berg_pipeline.definitions \
  --select route_pairs,train_types
```

The old command without `-m berg_pipeline.definitions` fails with “Invalid set of CLI arguments
for loading repository/job.” Dagster currently prints a supersession warning for this command,
but it remains the working CLI in the pinned environment.

Steady state is the monthly GitHub Actions workflow in `../.github/workflows/monthly-ingest.yml`.
It builds one source month, uploads its daily files, and publishes the updated manifest last.

## Publishing the local mirror

Load R2 credentials from the repository-root `.env`, preview the operation, then run it:

```sh
set -a
. ../.env
set +a

uv run python scripts/sync_publish.py --dry-run --delete
uv run python scripts/sync_publish.py --delete
```

Preflight requires leg/journey parity and geometry for every registered route. Uploads are
content-aware and resumable, transient R2 failures are retried, `manifest.json` is written last,
and obsolete managed objects are removed only when `--delete` is present. Rerunning after an
interruption safely skips objects already current.

## Things that will bite

All verified against a real day — details and numbers in [`../docs/data-notes.md`](../docs/data-notes.md).

- **Schema drift is the default, not the exception.** Read with `all_varchar=true` and parse
  explicitly. DuckDB's autodetect silently typed `BETRIEBSTAG` as DATE on a real file; across ten
  years that turns a schema change into a silent cast instead of an error.
- **`*_PROGNOSE` is only an observation when `*_PROGNOSE_STATUS` says so, and in v2 that value is
  `REAL`** — `GESCHAETZT` never appears for trains, only buses. Everything else is a forecast;
  never render a forecast as an observation. Fall back to scheduled and set `FLAG_SCHEDULED_FALLBACK`.
- **There is no stop-sequence column.** Order by *scheduled* time — a delayed train's actual times
  go non-monotonic and scramble the stop order.
- **`upper(PRODUKT_ID)`** — the feed contains both `Bus` and `BUS`.
- **Two timestamp formats coexist in one file**: `DD.MM.YYYY HH:MM` (scheduled) and
  `DD.MM.YYYY HH:MM:SS` (actual). `try_strptime` with a format list handles both.
- **A month does not fit in RAM.** `memory_limit` and `temp_directory` are set in `resources.py`;
  DuckDB spills rather than dies.
- **Timestamps are Europe/Zurich local.** Convert to UTC at ingest. The October DST day has 25
  hours and is the test case.
- **Usable coverage starts 2018-01**, not 2016 — 2016+2017 ship as one ZIP named *unvollständig*.
- **A file is not proof of a complete day.** The month validator enforces the census-expected
  days and a minimum of 10,000 legs for non-hole days before publication.
- **Both endpoints of a leg must use one clock basis.** Never combine measured departure with a
  scheduled arrival; the ingest contract falls back both endpoints together and sets a flag.
- **Bound duration before splitting.** Inputs above 24 hours are quarantined so a corrupt
  timestamp cannot amplify into millions of hourly sub-legs.
