# geometry

One-off job. Produces `routes.bin`: one polyline per station pair that trains actually run,
keyed by the `route_id` the pipeline assigned at ingest (the `station_pairs` table in
`data/berg.duckdb` is this job's input queue). The frontend ships it as a static asset and
interpolates along it.

The full-history build on 2026-07-20 contains 11,502 routes and 512,412 points in 2,153,214
bytes. `dim_route` verifies that every route ID referenced by the facts exists in the file and
fails loudly if geometry is stale.

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

The full-history run took 110 seconds: it loaded 402,921 nodes and 409,536 edges, snapped 1,894
of 2,148 referenced stations, and routed 11,502 pairs. A JSON report lands next to the output.

### Other countries

European datasets get their own `routes.bin`. Pass `--fit-bbox`: the uint16 grid is fitted to
the stations that dataset serves (a single Europe-wide box would cost most of the precision),
and the flat projection is re-centred on its latitude. The reader takes the grid from the file
header, so nothing else changes.

```sh
curl -sLo ../data/raw/finland-latest.osm.pbf \
    https://download.geofabrik.de/europe/finland-latest.osm.pbf
uv run python -m berg_geometry.build --fit-bbox \
    --pbf ../data/raw/finland-latest.osm.pbf \
    --db ../data/datasets/fi/berg.duckdb \
    --dim ../data/datasets/fi/dim_station.parquet \
    --out ../data/datasets/fi/publish/static/routes.bin
```

The Netherlands is the same with `netherlands-latest.osm.pbf` and `datasets/nl`.

Belgium (`datasets/be`) uses a rail-only Overpass export instead of the Geofabrik extract.
`--pbf` accepts any file pyosmium reads, including `.osm` XML, and the export arrives in
about two minutes where the full country extract crawls:

```sh
curl -s -A "berg-geometry/1.0" -o ../data/raw/belgium-rail.osm --data-urlencode \
    'data=[out:xml][timeout:600];area["ISO3166-1"="BE"][admin_level=2]->.be;way["railway"~"^(rail|narrow_gauge|light_rail)$"](area.be);(._;>;);out body;' \
    https://overpass-api.de/api/interpreter
```

Austria (`datasets/at`) uses the same kind of Overpass export, merged with the rail ways OSM
tags as under construction. In 2026 that tag covers the Wien S-Bahn Stammstrecke and
Feldkirch – Buchs, which carried trains throughout the window. Without them 6 stations cannot
be snapped and 26 routes fall back to straight lines:

```sh
curl -s -A "berg-geometry/1.0" -o ../data/raw/austria-rail.osm --data-urlencode \
    'data=[out:xml][timeout:600];area["ISO3166-1"="AT"][admin_level=2]->.a;way["railway"~"^(rail|narrow_gauge|light_rail)$"](area.a);(._;>;);out body;' \
    https://overpass-api.de/api/interpreter
curl -s -A "berg-geometry/1.0" -o ../data/raw/austria-rail-construction.osm --data-urlencode \
    'data=[out:xml][timeout:300];area["ISO3166-1"="AT"][admin_level=2]->.a;way["railway"="construction"]["construction"~"^(rail|light_rail)$"](area.a);(._;>;);out body;' \
    https://overpass-api.de/api/interpreter
sed -i 's|<tag k="railway" v="construction"/>|<tag k="railway" v="rail"/>|' \
    ../data/raw/austria-rail-construction.osm
uv run python -m berg_geometry.merge_osm ../data/raw/austria-rail.osm.pbf \
    ../data/raw/austria-rail.osm ../data/raw/austria-rail-construction.osm
uv run python -m berg_geometry.build --fit-bbox --pbf ../data/raw/austria-rail.osm.pbf \
    --db ../data/datasets/at/berg.duckdb --dim ../data/datasets/at/dim_station.parquet \
    --out ../data/datasets/at/publish/static/routes.bin                  # ~35 s
```

Germany (`datasets/de`) is too large for one Overpass query: every public instance times out
(HTTP 504, "server is probably too busy") on the whole country. Fetch eight tiles of about
2° × 4.7° from `overpass.kumi.systems` (each ~40-110 MB, ~4-5 min), then merge them. Tiles
share the ways that cross their seams, and the merge writes each node and way once:

```sh
for la in "47.2,49.2" "49.2,51.2" "51.2,53.2" "53.2,55.1"; do
  for lo in "5.8,10.5" "10.5,15.1"; do
    s=${la%,*}; n=${la#*,}; w=${lo%,*}; e=${lo#*,}
    curl -s -A "berg-geometry/1.0" -o ../data/raw/germany-rail-$s-$w.osm --data-urlencode \
      "data=[out:xml][timeout:900];way[\"railway\"~\"^(rail|narrow_gauge|light_rail)\$\"]($s,$w,$n,$e);(._;>;);out body;" \
      https://overpass.kumi.systems/api/interpreter
  done
done
uv run python -m berg_geometry.merge_osm ../data/raw/germany-rail.osm.pbf ../data/raw/germany-rail-*.osm
```

Retry a tile whose file does not end in `</osm>`: a 504 comes back as a 695-byte HTML page.

In practice the Munich tile never came back, so the published `de` geometry was built from the
Geofabrik extract, cut to its rail network first. Loading the 4.8 GB extract directly would
index every node location in memory, and 8 GB is not enough for that:

```sh
uv run python -m berg_geometry.merge_osm --extract \
    ../data/raw/germany-rail.osm.pbf ../data/raw/germany-latest.osm.pbf   # 37 s, 20 MB
uv run python -m berg_geometry.build --fit-bbox \
    --pbf ../data/raw/germany-rail.osm.pbf \
    --db ../data/datasets/de/berg.duckdb --dim ../data/datasets/de/dim_station.parquet \
    --out ../data/datasets/de/publish/static/routes.bin                  # ~20 min
```

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
cutting a corner beats a missing train. The current file has 1,347 flagged fallbacks: 948
unsnappable, 384 unreachable, and 15 rejected as absurd detours. The count is expected to be
larger for full-history data than for a single month: it includes foreign and cross-border
stations outside the *Switzerland* PBF plus disconnected rail components. Fallbacks are a
disclosed visual-quality limitation, not absent records.

Current routed distance ratios are p50 1.115, p99 2.543, and max 6.379. Snap distance is p50
9.3 m and p95 36.3 m.

`routes.bin` format lives in `src/berg_geometry/binfmt.py` (the authority); the pipeline has a
stdlib-only header reader (`berg_pipeline/routesbin.py`) and a byte-level contract test.

**Do not use OSRM.** Its profiles are road-shaped; it will route trains down streets.
