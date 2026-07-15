# infra

R2 bucket, CORS, and the monthly cron. Cost target is ~free: R2's free tier gives 10 GB of
storage and zero egress, which is the constraint the entire data model is built around.

## Bucket layout

```
r2://berg/
  legs/YYYY/MM/DD.parquet      one file per service day, ~3–8 MB
  static/routes.bin
  static/stations.json
  static/aggregates/*.json
  tiles/switzerland.pmtiles
  manifest.json                date range, file sizes, max_leg_duration, schema version
```

`manifest.json` is written **last**, so a half-finished backfill never advertises days that
aren't there.

## CORS — do this before debugging anything else

DuckDB WASM reads Parquet over HTTP range requests. Without CORS allowing `GET` plus the `range`
header, and without `content-range` exposed, it fails in a way that looks like a DuckDB bug and
is not.

```sh
npx wrangler r2 bucket cors put berg --file infra/cors.json
```

Verify before you trust it:

```sh
curl -sI -H "Origin: https://berg.ch" -H "Range: bytes=0-99" \
  https://data.berg.ch/legs/2024/01/15.parquet
# want: 206 Partial Content, access-control-allow-origin, content-range
```

## The Parquet contract

Row group size is the load-bearing tuning knob — it is literally the number of bytes downloaded
per scrub. Files are sorted by `t_dep` with row groups of about an hour of departures (~8–10k
rows), zstd, dictionary encoding on `route_id`/`type`, delta on `t_dep`, and per-file min/max
stats so footer pruning works.

If cold scrubs are slow, the fallback is weekly files with day-level row groups — fewer HTTP
handshakes. Decide that on a measurement, not a hunch.

## Budget

800M legs at 8 bytes is 6.4 GB against a 10 GB tier. Headroom is thin. Measure bytes/leg at M1
after one month is backfilled, not at the end. The escape hatch is dropping the oldest years;
overshoot costs cents, not the project.

## Backfill vs steady state

The ~1.8 TB backfill does not run in CI — run it locally or on a rented box over a weekend.
Steady state is `.github/workflows/monthly-ingest.yml`: one archive ZIP, one partition, ~150 MB
to R2, comfortably inside CI limits.
