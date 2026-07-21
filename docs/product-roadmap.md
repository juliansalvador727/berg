# Product roadmap: train observer

Updated 2026-07-20. This is the frontend/product direction after the historical data backfill.
The goal is a spatial train observer: find a train, jump to its departure, and follow it through a
detailed railway landscape. It is not a timeline scrubber.

## Implemented foundation

- Playback starts automatically at 600× on the latest substantial published day.
- The archive-wide range slider is removed. Ctrl/Cmd+K changes speed, pauses/resumes, jumps to a
  Europe/Zurich date/time, and searches train identities or lines on the selected day.
- Search results include the exact published journey identity, first departure time, and first
  station pair. Selecting one seeks to departure, highlights it, and follows it on the map.
- A staged startup progress bar covers DuckDB, metadata, route geometry, map style, and the first
  movement window.
- OpenFreeMap's dark vector style supplies clean cartography and city/place labels. MapLibre DEM
  hillshade adds mountain relief. Both are temporary hosted sources pending self-hosting.
- All 2,148 rail stations referenced by published movements are visible and clickable. A station
  panel queries observed arrivals/departures from 15 minutes behind to three simulated hours
  ahead.
- Service-family filters cover S-Bahn, regional, IC/IR, high-speed/international, night, and
  other trains. Service/delay coloring remains available.
- The 11,502 observed station-pair polylines render as a subdued railway network below trains.

## Next: GLB train fleet

Use deck.gl `ScenegraphLayer` with one instanced layer per model family. The initial registry
should map published service classes to six deliberately generic assets:

| Model family | Published types |
|---|---|
| S-Bahn EMU | `S`, `SN` |
| Regional EMU | `R`, `RB`, `RE`, `IRE`, `TER`, `PE` |
| InterCity consist | `IC`, `IR` |
| High-speed set | `ICE`, `TGV`, `EC`, `RJ`, `RJX` |
| Night consist | `NJ`, `EN`, `NZ` |
| Other rail | everything else |

Asset contract:

- `.glb`, meters, centered at track level, forward axis documented and identical across assets;
- low-poly silhouette that remains readable from zoom 12–16;
- one PBR material atlas per model, compressed textures, no external files;
- target under 250 KB compressed per family, with a point/marker fallback below zoom 11;
- position and bearing derived from the route tangent; selected trains get a scale/halo treatment;
- assets must have an explicit license compatible with public redistribution.

The current feed identifies a service class, not a physical vehicle or formation. Generic
service-family models are honest. Exact Giruno, FLIRT, IC2000, Re 460, and similar assignments
need an additional rolling-stock/formation source joined by journey and date; do not infer them
from `PRODUKT_ID`.

## Next: complete rail infrastructure tiles

`routes.bin` contains paths for observed station pairs. It is enough for movement interpolation,
but it is not a complete railway basemap and it discards segment semantics. Build a separate
OSM-to-PMTiles job from Switzerland plus a cross-border buffer, retaining at least:

- `railway`, `usage`, `service`, `gauge`, `electrified`, and `maxspeed`;
- `tunnel`, `bridge`, `layer`, and `cutting`;
- station/platform geometry, names, operator, and network where available.

Zoom styling:

- z6–8: mainline hierarchy only;
- z9–11: regional lines, cities, major stations, terrain;
- z12–14: all running tracks, tunnels/bridges, station names;
- z15+: individual tracks, platforms, yards, switches where source geometry supports them.

Tunnels should be dashed/dimmed with portal emphasis; bridges should remain solid and raised in
the visual hierarchy. This must come from OSM segment tags—tunnel state cannot be reconstructed
from the current station-pair geometry.

## Next: production terrain and basemap

Replace public demo/hosted sources with project-controlled PMTiles and DEM assets before launch.
Use hillshade at national zooms and optional 3D terrain only at closer zooms after testing train
alignment, label readability, mobile GPU cost, and attribution. City/place labels should remain
above terrain and rail layers.

## Search and station-board expansion

Current search intentionally queries the selected UTC day's journey sidecar. Use Ctrl/Cmd+K to
jump to a local date/time, then search that day. Archive-global autocomplete would otherwise scan
thousands of remote Parquet files; add a compact search index keyed by date, normalized train
number, line, origin, destination, and first departure.

Current station boards contain observed published movements and are therefore appropriate for
historical playback. A timetable-grade board with cancellations, through services, platform,
formation, and stops requires publishing a richer journey/call sidecar. Keep “observed” visibly
different from “scheduled” rather than filling unavailable fields with guesses.

## Delivery order

1. Human smoke-test and correct the new observer UI at `http://localhost:5173`.
2. Create/license the six generic GLBs and replace markers at close zoom.
3. Build complete semantic rail PMTiles, including tunnel and bridge attributes.
4. Self-host the dark basemap and DEM; tune zoom-dependent detail.
5. Publish archive search indexes and richer journey/call sidecars.
6. Measure desktop/mobile performance, then add LOD and GPU optimizations where measured.

Implementation references: [OpenFreeMap styles](https://openfreemap.org/quick_start/),
[MapLibre hillshade](https://maplibre.org/maplibre-gl-js/docs/examples/add-a-hillshade-layer/),
and [deck.gl ScenegraphLayer](https://deck.gl/docs/api-reference/mesh-layers/scenegraph-layer).
