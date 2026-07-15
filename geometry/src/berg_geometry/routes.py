"""Shortest paths per observed station pair → simplified, quantized polylines."""

from pathlib import Path

import networkx as nx

# Douglas-Peucker tolerance. At ~10 m, a polyline is visually identical at max zoom and a
# fraction of the vertices.
SIMPLIFY_TOLERANCE_M = 10.0


def observed_pairs(duckdb_path: Path) -> list[tuple[int, int]]:
    """The ~20k distinct ordered (from_bpuic, to_bpuic) pairs that actually appear in the facts.

    Only pairs trains actually run — not the cross product, which is 100x bigger and mostly
    nonsense.
    """
    raise NotImplementedError("M2: SELECT DISTINCT from fct_legs")


def route_polylines(
    graph: nx.Graph,
    snapped: dict[int, int],
    pairs: list[tuple[int, int]],
) -> dict[tuple[int, int], list[tuple[float, float]]]:
    """One shortest path per pair, computed once, ever.

    Unroutable pairs fall back to a straight line and are flagged — a missing train is worse
    than a train that cuts a corner.
    """
    raise NotImplementedError("M2: nx.shortest_path per pair + simplify")


def write_routes_bin(
    polylines: dict[tuple[int, int], list[tuple[float, float]]],
    out_path: Path,
) -> None:
    """Pack polylines into routes.bin, keyed by route_id (uint32).

    Coordinates are quantized to uint16 within CH_BBOX: ~7 m resolution across Switzerland,
    which is under the simplify tolerance and therefore free.
    """
    raise NotImplementedError("M2: pack + emit route_id lookup for the pipeline")
