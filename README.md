# berg

Historical Swiss train movements, replayed in the browser.

The serving path has no application backend: daily Parquet files live on Cloudflare R2,
DuckDB WASM queries them in a Web Worker, and MapLibre/deck.gl renders trains in the browser.
Measured arrival and departure times anchor each train at intermediate stops rather than only
at the ends of a trip.

## Status

The full usable Ist-Daten archive has been rebuilt and published. As of 2026-07-20:

- 422,103,769 legs across 3,090 UTC day files
- source coverage from 2018-01 through 2026-06 (manifest range 2017-12-31 through 2026-07-01)
- 11,502 station-pair routes with a geometry for every route ID
- schema version 3, with matching local and R2 leg/journey inventories
- 15 manifest days explicitly marked missing; no partial day is advertised

The pipeline and publication work are complete for the historical backfill. The frontend now has
the train-observer foundation: automatic 600× playback, command-palette train/date search,
spectating, service filters, clickable observed station boards, and dark terrain-aware mapping.
The immediate next work is its interactive smoke test, deployment, GLB fleet assets, semantic
rail/tunnel tiles, and production-owned basemap/terrain data.
See [`current_state.md`](current_state.md) for the authoritative checklist and known limitations.

## Layout

| Directory | What it is |
|---|---|
| `pipeline/` | Dagster + DuckDB: Ist-Daten archives to daily leg/journey Parquet and R2 |
| `geometry/` | OSM rail graph to one shared polyline per station pair |
| `web/` | Vite + TypeScript + MapLibre + deck.gl + DuckDB WASM frontend |
| `infra/` | R2 CORS configuration and the monthly GitHub Actions workflow |
| `docs/` | Measured archive/schema findings and the source coverage census |

## Quick start

The R2 CORS policy permits the exact local origin `http://localhost:5173`:

```sh
cd web
npm install
npm run dev -- --host localhost --port 5173
```

Open <http://localhost:5173>. Do not substitute `127.0.0.1`; it is a different browser origin
and is intentionally not in the production bucket's CORS allowlist.

Pipeline development:

```sh
cd pipeline
uv sync
uv run pytest
uv run ruff check .
uv run dagster dev -m berg_pipeline.definitions
```

The production dataset is already published. Rebuild and sync procedures are in
[`pipeline/README.md`](pipeline/README.md); do not start a historical backfill for ordinary
frontend work.

## Documentation

- [`current_state.md`](current_state.md): current numbers, validation evidence, known issues,
  cleanup, and next work
- [`docs/data-notes.md`](docs/data-notes.md): measured source-data behavior and design decisions
- [`docs/product-roadmap.md`](docs/product-roadmap.md): observer UI, GLB, detailed rail map,
  terrain, search, and station-board plan
- [`infra/README.md`](infra/README.md): bucket contract, CORS, and steady-state operations

## Attribution

- Timetable and actual-movement data: [opentransportdata.swiss](https://opentransportdata.swiss)
- Rail geometry and map data: [OpenStreetMap contributors](https://www.openstreetmap.org/copyright),
  ODbL
- Planned production basemap packaging: [Protomaps](https://protomaps.com)
