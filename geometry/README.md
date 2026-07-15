# geometry

One-off job. Produces `routes.bin`: one polyline per station pair that trains actually run,
keyed by the `route_id` the pipeline assigned at ingest (the `station_pairs` table in
`data/berg.duckdb` is this job's input queue). The frontend ships it as a static asset and
lerps along it — measured 463 KB for the 4,614 pairs of 2018-05, so the ~20 MB the plan
budgeted has enormous headroom.

Slow and rare — the OSM rail graph changes far less than the timetable. Rerun it when new
station pairs show up (the pipeline's `dim_route` asset fails loudly when facts reference
route_ids this job hasn't seen) or when you want fresher OSM data.

## Run

```sh
uv sync
curl -sLo ../data/raw/switzerland-latest.osm.pbf \
    https://download.geofabrik.de/europe/switzerland-latest.osm.pbf
uv run python -m berg_geometry.build \
    --pbf ../data/raw/switzerland-latest.osm.pbf \
    --db ../data/berg.duckdb \
    --dim ../data/dim_station.parquet \
    --out ../data/publish/static/routes.bin
```

~3 minutes total: ~45 s PBF scan, ~90 s routing. A JSON report lands next to the output.

## How it works

1. Load `railway=rail|narrow_gauge|light_rail` ways from the Geofabrik extract into a weighted
   graph (pyosmium with a C-side location index; scipy CSR + KDTree — **not** pyrosm/networkx,
   which have no 3.12 wheels and would take hours where this takes minutes).
2. Snap each station to nearby rail nodes **per connected component**, ceiling 300 m. Nearest
   node alone is wrong: at multi-gauge stations (Interlaken Ost, Montreux) it picks a network
   at random and the "shortest path" detours 187 km via Luzern. Both ends of a pair must land
   in the same component, chosen to minimize combined snap distance.
3. One Dijkstra per unique source node (scipy, C), paths rebuilt from the predecessor array.
4. Cap the polyline ends with the station coordinates (legs must meet exactly at station dots),
   simplify (Douglas-Peucker, 10 m, in flat CH-meter space) and quantize to uint16 within the
   CH bounding box — ~7 m resolution, below the simplify tolerance, so it costs nothing.

Unroutable pairs get a straight line and a flag (`binfmt.FLAG_STRAIGHT_FALLBACK`). A train
cutting a corner beats a missing train. The real fallbacks are almost all the Italian legs of
the Simplon/Centovalli lines — inside the map bbox but outside the *Switzerland* PBF extract,
so there is literally no track data to route on.

`routes.bin` format lives in `src/berg_geometry/binfmt.py` (the authority); the pipeline has a
stdlib-only header reader (`berg_pipeline/routesbin.py`) and a byte-level contract test.

**Do not use OSRM.** Its profiles are road-shaped; it will route trains down streets.
