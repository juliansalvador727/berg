# berg

Every measured Swiss train movement since 2016, replayed in the browser.

No backend in the serving path: Parquet on object storage, DuckDB WASM in the browser, GPU
rendering. The source data ([Ist-Daten](https://opentransportdata.swiss)) records measured times at
every intermediate stop, so interpolated train positions are anchored to reality at every station.

## Layout

| Directory   | What it is                                                                       |
| ----------- | -------------------------------------------------------------------------------- |
| `pipeline/` | Dagster + DuckDB: Ist-Daten archives → per-day leg Parquet → R2                   |
| `geometry/` | One-off OSM job: rail graph → per-station-pair polylines → `routes.bin`           |
| `web/`      | Vite + TypeScript + MapLibre + deck.gl + DuckDB WASM frontend                     |
| `infra/`    | R2 bucket setup, CORS config, GitHub Actions workflows                            |

Each directory has its own README with setup and run instructions.

## Quick start

```sh
# Pipeline
cd pipeline && uv sync && uv run dagster dev

# Frontend
cd web && npm install && npm run dev
```

## Status

Pre-M0. Nothing works yet; the skeleton is in place and every asset is a stub.

## Attribution

- Timetable and actual-movement data: [opentransportdata.swiss](https://opentransportdata.swiss),
  CC BY-style — see the About page.
- Rail geometry: [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors, ODbL.
- Basemap tiles: [Protomaps](https://protomaps.com), OpenStreetMap data.
