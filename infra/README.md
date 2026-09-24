# infra

R2 bucket, CORS, and the monthly cron. Cost target is ~free: R2's free tier gives 10 GB of
storage and zero egress, which is the constraint the entire data model is built around.

## Bucket layout

```
r2://berg/
  legs/YYYY/MM/DD.parquet      compact movement facts, one file per UTC day
  journeys/YYYY/MM/DD.parquet  journey identity/metadata sidecar for the same day
  static/routes.bin
  static/route_pairs.json
  static/stations.json
  static/train_types.json
  static/routes.report.json
  static/duckdb-wasm/1.32.0/*.wasm
  tiles/switzerland.pmtiles    planned; not published yet
  manifest.json                date range, file sizes, max_leg_duration, schema version
  catalog.json                 every dataset: path, timezone, bbox, coverage, licence
  datasets/<id>/               one European dataset, same layout as the Swiss root:
    legs/ journeys/ static/ manifest.json
```

Switzerland stays at the bucket root with schema v3 unchanged; it is listed in
`catalog.json` with `"path": ""`. Every other country is a self-contained dataset under
`datasets/<id>/` with its own route, station, journey and type id spaces. Clients that never
read the catalog keep working on the Swiss root manifest. `catalog.json` is written after the
dataset's own manifest, so a dataset is only discoverable once its files are live.
Publish a dataset with `pipeline/scripts/sync_dataset.py <id>`; it refuses to upload if the
dataset exceeds its allocation or the projected bucket would exceed 9.0 GB (see `europe.md`).

`manifest.json` is written **last**, so a half-finished backfill never advertises days that
aren't there.

The DuckDB WASM modules exceed Cloudflare Pages' per-file size limit. After `npm ci` in `web/`,
publish the pinned runtime modules directly to R2:

```sh
set -a; source .env; set +a
cd pipeline
uv run python scripts/upload_web_runtime.py
```

## CORS — do this before debugging anything else

DuckDB WASM reads Parquet over HTTP range requests. Without CORS allowing `GET` plus the `range`
header, and without `content-range` exposed, it fails in a way that looks like a DuckDB bug and
is not.

```sh
npx wrangler r2 bucket cors set berg --file infra/cors.json
```

Verify before you trust it:

```sh
curl -sI -H "Origin: https://berg-rail-observer.pages.dev" -H "Range: bytes=0-99" \
  https://pub-40f06e4404c049578963083898f4ab57.r2.dev/legs/2024/01/15.parquet
# want: 206 Partial Content, access-control-allow-origin, content-range
```

`infra/cors.json` also permits the exact development origin `http://localhost:5173`. Browser
origins are exact: `http://127.0.0.1:4173` is different and will be rejected. Run local frontend
smoke tests on localhost port 5173 rather than weakening the production allowlist.

## The Parquet contract

Row group size is the load-bearing tuning knob — it is literally the number of bytes downloaded
per scrub. Files are sorted by `t_dep` with row groups of about an hour of departures (~8–10k
rows), zstd, dictionary encoding on `route_id`/`type`, delta on `t_dep`, and per-file min/max
stats so footer pruning works.

If cold scrubs are slow, the fallback is weekly files with day-level row groups — fewer HTTP
handshakes. Decide that on a measurement, not a hunch.

## Budget

The completed historical mirror is about 3.7 GB plus its manifest for 422,103,769 legs, including
the journey sidecars and static assets. It is comfortably below the 10 GB free-tier storage
allowance. Continue checking bucket size as new months arrive; the old 800M-leg/6.4 GB figure was
a planning estimate, not a measurement.

## Backfill vs steady state

The measured ~1.27 TB raw historical backfill is complete and does not run in CI. Steady state is
`.github/workflows/monthly-ingest.yml`: one archive ZIP and one month build. Before relying on the
schedule, manually dispatch it for a known month and verify upload, manifest-last behavior, and
rerun idempotency.
