"""OSM railway=rail → routable graph.

Deliberately not OSRM: its profiles are road-shaped and will happily route a train down a
street. We want the rail graph and nothing else.
"""

from pathlib import Path

import networkx as nx

GEOFABRIK_URL = "https://download.geofabrik.de/europe/switzerland-latest.osm.pbf"


def build_rail_graph(pbf_path: Path) -> nx.Graph:
    """Load railway=rail ways from a Geofabrik extract into a weighted graph.

    Edge weight is metric length; nodes are OSM node ids with (lon, lat) attributes.
    """
    raise NotImplementedError("M2: pyrosm → networkx")


def snap_stations(graph: nx.Graph, stations: list[dict]) -> dict[int, int]:
    """Map each bpuic to its nearest rail node.

    Stations sit next to the track, not on it, so 'nearest' needs a distance ceiling — a
    station that snaps 800 m away is a bug (bus stop, closed line, bad coords), not a route.
    """
    raise NotImplementedError("M2: spatial index + nearest-node with a sanity ceiling")
