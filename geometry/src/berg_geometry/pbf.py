"""Geofabrik extract → the raw rail network as flat arrays.

One pass with a C-side location index: pyosmium resolves way-node coordinates itself, so
Python only sees the ~100k rail ways, not the ~60M nodes.
"""

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import osmium

# What PRODUKT_ID='Zug' actually runs on. 'rail' is the standard network; 'narrow_gauge' is
# RhB/MGB/zb and friends, which are trains here, not trams; 'light_rail' catches suburban
# lines on the boundary (Forchbahn). Trams, funiculars, and rack-only products are filtered
# out of the facts at ingest, so their tracks would only invite wrong shortcuts.
RAIL_TYPES = frozenset({"rail", "narrow_gauge", "light_rail"})

# railway=abandoned/razed/construction are different VALUES and never match RAIL_TYPES, but
# a live value can still be lifecycled with a side tag (railway=rail + disused=yes).
_DEAD_TAGS = ("disused", "abandoned", "razed")


@dataclass
class RailNetwork:
    """The network as parallel arrays: nodes indexed 0..n-1, edges as index pairs."""

    node_ids: np.ndarray  # int64 OSM ids, for debugging only
    lons: np.ndarray  # float64
    lats: np.ndarray  # float64
    edges: np.ndarray  # (m, 2) int32 node indices, undirected

    @property
    def n_nodes(self) -> int:
        return len(self.node_ids)


class _RailWays(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.chains: list[list[tuple[int, float, float]]] = []

    def way(self, w):
        if w.tags.get("railway") not in RAIL_TYPES:
            return
        if any(w.tags.get(t) == "yes" for t in _DEAD_TAGS):
            return
        chain = [(n.ref, n.location.lon, n.location.lat) for n in w.nodes if n.location.valid()]
        if len(chain) >= 2:
            self.chains.append(chain)


def load_rail_network(pbf_path: Path) -> RailNetwork:
    handler = _RailWays()
    handler.apply_file(str(pbf_path), locations=True)

    id_to_idx: dict[int, int] = {}
    node_ids, lons, lats = [], [], []
    edges: list[tuple[int, int]] = []
    for chain in handler.chains:
        prev = None
        for ref, lon, lat in chain:
            idx = id_to_idx.get(ref)
            if idx is None:
                idx = id_to_idx[ref] = len(node_ids)
                node_ids.append(ref)
                lons.append(lon)
                lats.append(lat)
            if prev is not None and prev != idx:
                edges.append((prev, idx))
            prev = idx

    return RailNetwork(
        node_ids=np.asarray(node_ids, dtype=np.int64),
        lons=np.asarray(lons, dtype=np.float64),
        lats=np.asarray(lats, dtype=np.float64),
        edges=np.asarray(edges, dtype=np.int32).reshape(-1, 2),
    )
