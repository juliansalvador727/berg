"""Merge overlapping OSM extracts into one file the geometry job can read.

Overpass times out on a whole-country rail query for Germany, so the export is fetched in
tiles (see README). Tiles share every node and way that crosses their seam; this writes each
id once, nodes before ways, so `load_rail_network` resolves every location in one pass.

    uv run python -m berg_geometry.merge_osm out.osm.pbf tile1.osm tile2.osm ...
    uv run python -m berg_geometry.merge_osm --extract out.osm.pbf germany-latest.osm.pbf
"""

import sys
from pathlib import Path

import osmium


def merge(out: Path, inputs: list[Path]) -> dict:
    if out.exists():
        out.unlink()
    writer = osmium.SimpleWriter(str(out))
    seen_nodes: set[int] = set()
    seen_ways: set[int] = set()
    try:
        for path in inputs:
            for obj in osmium.FileProcessor(str(path), osmium.osm.NODE):
                if obj.id not in seen_nodes:
                    seen_nodes.add(obj.id)
                    writer.add_node(obj)
        for path in inputs:
            for obj in osmium.FileProcessor(str(path), osmium.osm.WAY):
                if obj.id not in seen_ways:
                    seen_ways.add(obj.id)
                    writer.add_way(obj)
    finally:
        writer.close()
    return {"nodes": len(seen_nodes), "ways": len(seen_ways), "bytes": out.stat().st_size}


def extract_rail(out: Path, pbf: Path) -> dict:
    """A country extract → only its rail ways and their nodes, in two passes.

    `load_rail_network` resolves node locations with an in-memory index over every node in
    the file, which a 4.8 GB country extract does not fit on an 8 GB machine. Ways first
    (collecting the node ids they need), then only those nodes, keeps memory to the rail
    network itself.
    """
    from berg_geometry.pbf import RAIL_TYPES

    ways = []
    needed: set[int] = set()
    rail_filter = osmium.filter.KeyFilter("railway")
    for w in osmium.FileProcessor(str(pbf), osmium.osm.WAY).with_filter(rail_filter):
        if w.tags.get("railway") in RAIL_TYPES:
            refs = [n.ref for n in w.nodes]
            needed.update(refs)
            ways.append((w.id, refs, dict(w.tags)))
    if out.exists():
        out.unlink()
    writer = osmium.SimpleWriter(str(out))
    nodes = 0
    try:
        # The id filter runs in C; a Python-side test over ~600M nodes would take an hour.
        node_filter = osmium.filter.IdFilter(needed)
        for n in osmium.FileProcessor(str(pbf), osmium.osm.NODE).with_filter(node_filter):
            writer.add_node(osmium.osm.mutable.Node(id=n.id, location=n.location))
            nodes += 1
        for wid, refs, tags in ways:
            writer.add_way(osmium.osm.mutable.Way(id=wid, nodes=refs, tags=tags))
    finally:
        writer.close()
    return {"nodes": nodes, "ways": len(ways), "bytes": out.stat().st_size}


if __name__ == "__main__":
    if sys.argv[1] == "--extract":
        print(extract_rail(Path(sys.argv[2]), Path(sys.argv[3])))
    else:
        print(merge(Path(sys.argv[1]), [Path(p) for p in sys.argv[2:]]))
