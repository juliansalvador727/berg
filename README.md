# berg

Historical European train movements, replayed in the browser.

berg started as a Swiss train watcher and now covers Switzerland, Finland, the Netherlands,
Belgium, Germany and Austria. Each country is its own dataset with its own source, coverage and
time semantics. A cross-border layer links them so a train can be followed from one country
into the next.

The serving path has no application backend: daily Parquet files live on Cloudflare R2,
DuckDB WASM queries them in a Web Worker, and MapLibre/deck.gl renders trains in the browser.
Where the source has them, measured arrival and departure times anchor each train at
intermediate stops rather than only at the ends of a trip.

## Datasets

| Country | Source | Coverage | Legs | Times |
|---|---|---|---:|---|
| Switzerland | opentransportdata.swiss Ist-Daten | 2018-01 to 2026-08 | 431.3M | observed |
| Finland | Fintraffic / Digitraffic | 2023-01-01 to 2026-08-30 | 17.5M | observed |
| Netherlands | Rijden de Treinen | 2023-01-01 to 2026-08-30 | 59.9M | scheduled + reported delay |
| Belgium | Infrabel punctuality files | 2023-01-01 to 2026-08-30 | 46.0M | observed |
| Germany | DB IRIS via piebro/deutsche-bahn-data | 2025-11-03 to 2026-08-30 | 124.2M | final prediction |
| Austria | ÖBB-Infrastruktur train runs + ÖBB-PV GTFS | 2025-12-15 to 2026-09-05 | 16.0M | scheduled + interpolated delay |

All six overlap from 2025-12-15 to 2026-08-30, and the viewer opens on the first day they all
share. Outside a country's coverage the map says so instead of showing an empty network.

The "Times" column matters. Observed data is what the train actually did. The Netherlands only
publishes the last reported delay in whole minutes, Germany's archive holds the last prediction
the crawler saw, and Austria only has actual times at the first and last operating point of each
run, so stop times in between are interpolated. The UI labels each of these, and where two
datasets hold the same movement near a border, only the copy with stronger evidence is drawn.

Switzerland was not rebuilt for any of this. It stays at the bucket root with its original ids,
and the other countries live under `datasets/<id>/`. A root `catalog.json` lists them all.

Italy and Poland are being fetched but are not built yet. Most other European countries have no
downloadable per-stop history; `europe.md` records what was checked.

## Layout

| Path | What it is |
|---|---|
| `pipeline/` | Dagster + DuckDB: source archives to daily leg/journey Parquet and R2 |
| `pipeline/src/berg_pipeline/europe/` | Per-country adapters, the shared stop-to-leg builder, catalog and cross-border links |
| `geometry/` | OSM rail graph to one shared polyline per station pair |
| `web/` | Vite + TypeScript + MapLibre + deck.gl + DuckDB WASM frontend |
| `infra/` | R2 CORS configuration and the monthly GitHub Actions workflow |
| `docs/` | Measured source-data findings per country, the cross-border rules and coverage censuses |

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

Every dataset is already published. Do not start a rebuild for ordinary frontend work.

Swiss rebuild and sync procedures are in [`pipeline/README.md`](pipeline/README.md). Each
European country follows the same pattern:

```sh
cd pipeline
uv run python scripts/build_nl.py --fetch   # or build_be / build_de / build_at; Finland uses fetch_fi.py then build_fi.py
# run the geometry job for that country (see geometry/README.md)
uv run python scripts/sync_dataset.py nl
```

After any dataset changes, rebuild the cross-border layer with `scripts/build_links.py` and
`scripts/sync_links.py`.

The whole bucket has to stay inside the Cloudflare R2 free tier (10 GB). Each dataset has a size
cap and `sync_dataset.py` refuses to upload past it. It is currently around 5.6 GB.

## Documentation

- [`current_state.md`](current_state.md): current numbers, validation, known issues and next work
- [`europe.md`](europe.md): the European plan, R2 budget, identity model and country research
- [`docs/cross-border.md`](docs/cross-border.md): station crosswalk, dedup and journey links
- [`docs/data-notes.md`](docs/data-notes.md): Swiss source-data behavior and design decisions
- `docs/data-notes-{fi,nl,be,de,at}.md`: the same for each European dataset
- [`docs/product-roadmap.md`](docs/product-roadmap.md): observer UI, GLB, detailed rail map,
  terrain, search and station-board plan
- [`infra/README.md`](infra/README.md): bucket contract, CORS and steady-state operations

## Attribution

- Switzerland: [opentransportdata.swiss](https://opentransportdata.swiss)
- Finland: [Fintraffic / Digitraffic](https://www.digitraffic.fi), CC BY 4.0
- Netherlands: [Rijden de Treinen](https://www.rijdendetreinen.nl), CC BY 4.0
- Belgium: [Infrabel](https://opendata.infrabel.be), CC0
- Germany: Deutsche Bahn Timetables and StaDa APIs via
  [piebro/deutsche-bahn-data](https://huggingface.co/datasets/piebro/deutsche-bahn-data), CC BY 4.0
- Austria: ÖBB-Infrastruktur (CC BY 3.0 AT) and ÖBB-Personenverkehr (CC BY 4.0)
- Rail geometry and map data: [OpenStreetMap contributors](https://www.openstreetmap.org/copyright),
  ODbL
- Planned production basemap packaging: [Protomaps](https://protomaps.com)
