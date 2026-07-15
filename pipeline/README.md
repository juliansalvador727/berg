# pipeline

Dagster + DuckDB. Turns monthly Ist-Daten archive ZIPs into one Parquet file per service day on R2.

## Setup

```sh
uv sync
cp ../.env.example ../.env   # fill in R2 credentials
uv run dagster dev           # http://localhost:3000
```

## Asset graph

```
raw_zip[month] → stg_istdaten[month] → fct_legs[month] → legs_parquet[day] → manifest
                                          ↑                    ↓
dim_station ──────────────────────────────┤              aggregates_json
dim_route (from geometry/) ───────────────┘
```

`dim_station` first — it is the hardest part. It is SCD Type 2 because a 2017 stop event has to
join to the 2017 name and coordinates, and stations get renamed, opened, and closed constantly.

## Running the backfill

~1.8 TB of raw CSV will not run in GitHub Actions. Run it once locally or on a rented box over a
weekend. Dagster partitions make it resumable, so a crash costs you one month, not the archive.
Process month by month and delete each raw ZIP as you go — you never need more than one month of
raw on disk.

```sh
uv run dagster asset materialize --select fct_legs --partition 2024-01
```

Steady state is a monthly GitHub Actions cron pulling one new archive: ~150 MB to R2, well inside
CI limits. See `infra/`.

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
