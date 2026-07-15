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

# Frontend (needs the M0 export below to exist)
cd web && npm install && npm run dev
```

## Status: M0 done — trains move

One real service day (2026-06-03) replays in the browser: 153,771 legs, 1,820 stations, straight-line
geometry, playback at 1×–600× with pause and scrub. That closes the loop end to end and retires the
integration risk.

Rebuild the M0 data (needs a day of Ist-Daten and a GTFS `stops.txt` in `data/raw/` — see
`docs/data-notes.md` for how to pull them without downloading whole archives):

```sh
cd pipeline && uv run python scripts/m0_export.py \
  --raw ../data/raw/2026-06-03_IstDaten.csv \
  --stops ../data/raw/stops-2026.txt \
  --out ../web/public/m0
```

Everything past M0 is still a stub. **[`docs/data-notes.md`](docs/data-notes.md) is the important
document** — it records what the data actually does, measured rather than assumed, and several of
its findings overturned the original design assumptions (measured status is `REAL`, 2016–17 is
unusable, and the storage budget has 3× more headroom than feared).

## Attribution

- Timetable and actual-movement data: [opentransportdata.swiss](https://opentransportdata.swiss),
  CC BY-style — see the About page.
- Rail geometry: [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors, ODbL.
- Basemap tiles: [Protomaps](https://protomaps.com), OpenStreetMap data.
