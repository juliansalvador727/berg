# geometry

One-off job. Produces `routes.bin`: one polyline per station pair that trains actually run,
keyed by `route_id`. The pipeline joins to it; the frontend ships it as a static asset (~20 MB)
and lerps along it.

Slow and rare — the OSM rail graph changes far less than the timetable. Rerun it when new station
pairs show up (the pipeline enqueues them) or when you want fresher OSM data.

## Run

```sh
uv sync
curl -O https://download.geofabrik.de/europe/switzerland-latest.osm.pbf
uv run python -m berg_geometry.build --pbf switzerland-latest.osm.pbf --out ../data/routes.bin
```

## How it works

1. Load `railway=rail` ways from the Geofabrik extract into a weighted graph.
2. Snap each station to its nearest rail node (with a distance ceiling — a station that snaps
   800 m away is bad data, not a route).
3. Shortest path for each of the ~20k observed `(from_bpuic, to_bpuic)` pairs.
4. Simplify (Douglas-Peucker, ~10 m) and quantize coordinates to uint16 within the CH bounding
   box — ~7 m resolution, below the simplify tolerance, so it costs nothing visually.

Unroutable pairs get a straight line and a flag. A train cutting a corner beats a missing train.

**Do not use OSRM.** Its profiles are road-shaped; it will route trains down streets.
