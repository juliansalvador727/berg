"""Build the cross-border layer: station crosswalk, journey links, bridge legs and geometry.

    uv run python scripts/build_links.py            # links, then bridge geometry
    uv run python scripts/sync_links.py             # R2: links/, then the catalog

Reads every dataset's local publish mirror (Switzerland's data/publish and each
data/datasets/<id>/publish), so rebuild it after any dataset changes. Bridge geometry is
routed by the ordinary geometry job over the merged rail networks of the countries a bridge
joins; the rail extracts are the ones each dataset's own geometry used (geometry/README.md).
"""

import argparse
import json
import subprocess
import sys
from datetime import date

from berg_pipeline import paths
from berg_pipeline.europe import links

GEOMETRY = paths.REPO_ROOT / "geometry"
RAW = paths.DATA_ROOT / "raw"
# Rail-only inputs per country. Switzerland only has the full extract, cut to its rail first.
RAIL_INPUTS = {
    "ch": RAW / "switzerland-rail.osm.pbf",
    "de": RAW / "germany-rail.osm.pbf",
    "nl": RAW / "netherlands-rail.osm",
    "be": RAW / "belgium-rail.osm",
    # The Overpass rail export merged with the lines OSM tags as under construction in 2026
    # (Wien Stammstrecke, Feldkirch - Buchs), which carried trains in the window.
    "at": RAW / "austria-rail.osm.pbf",
}


def geometry_python(*args: str) -> None:
    subprocess.run([str(GEOMETRY / ".venv" / "bin" / "python"), *args], cwd=GEOMETRY, check=True)


def build_geometry(root) -> dict:
    swiss = RAIL_INPUTS["ch"]
    if not swiss.exists():
        geometry_python("-m", "berg_geometry.merge_osm", "--extract", str(swiss),
                        str(RAW / "switzerland-latest.osm.pbf"))
    manifest = json.loads((root / "publish" / "manifest.json").read_text())
    countries = sorted({ds for pair in manifest["bridge_pairs"] if pair["accepted"]
                        for ds in (pair["from"][0], pair["to"][0])})
    merged = root / "geometry" / "rail.osm.pbf"
    geometry_python("-m", "berg_geometry.merge_osm", str(merged),
                    *[str(RAIL_INPUTS[c]) for c in countries])
    out = root / "publish" / "static" / "routes.bin"
    geometry_python("-m", "berg_geometry.build", "--fit-bbox", "--pbf", str(merged),
                    "--db", str(root / "geometry" / "bridges.duckdb"),
                    "--dim", str(root / "geometry" / "dim.parquet"), "--out", str(out))
    return json.loads(out.with_suffix(".report.json").read_text())


def main(first: date | None, last: date | None, skip_geometry: bool) -> int:
    manifest = links.build(links.LINKS_ROOT, first, last)
    print(json.dumps({k: manifest[k] for k in ("counts", "bridge_routes")}))
    if not skip_geometry and manifest["bridge_routes"]:
        report = build_geometry(links.LINKS_ROOT)
        print("bridge geometry:", {k: report[k] for k in ("routes", "fallback_routes", "bytes")})
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--first", type=date.fromisoformat)
    p.add_argument("--last", type=date.fromisoformat)
    p.add_argument("--skip-geometry", action="store_true")
    a = p.parse_args()
    sys.exit(main(a.first, a.last, a.skip_geometry))
