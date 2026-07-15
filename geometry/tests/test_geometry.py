"""The geometry contract on a hand-drawn mini rail network.

Layout (lon/lat, roughly 1 unit ≈ degrees but distances only matter relatively):

    A(8.0,47.0) --- B(8.1,47.0) --- C(8.2,47.0)     a straight east-west line
                     |
                     D(8.1,47.1)                     a branch north from B
    E(9.0,46.0)                                      an isolated node (own component)
"""

from pathlib import Path

import numpy as np
import pytest

from berg_geometry.binfmt import (
    FLAG_STRAIGHT_FALLBACK,
    dequantize,
    quantize,
    read_routes_bin,
    write_routes_bin,
)
from berg_geometry.graph import RailGraph
from berg_geometry.pbf import RailNetwork
from berg_geometry.proj import to_meters
from berg_geometry.routes import Pair, route_all, simplify_polyline


@pytest.fixture
def graph() -> RailGraph:
    lons = np.array([8.0, 8.1, 8.2, 8.1, 9.0])
    lats = np.array([47.0, 47.0, 47.0, 47.1, 46.0])
    edges = np.array([[0, 1], [1, 2], [1, 3]], dtype=np.int32)
    net = RailNetwork(node_ids=np.arange(5, dtype=np.int64), lons=lons, lats=lats, edges=edges)
    return RailGraph.build(net)


def pair(route_id, f, t, coords):
    return Pair(route_id, f, t, coords[f][0], coords[f][1], coords[t][0], coords[t][1])


# Stations sit a few meters off their nodes; station 99 is nowhere near track.
COORDS = {
    1: (8.0001, 47.0001),  # near A
    2: (8.1001, 47.0001),  # near B
    3: (8.2001, 47.0001),  # near C
    4: (8.1001, 47.1001),  # near D
    5: (9.0001, 46.0001),  # near E (isolated component)
    99: (8.5, 47.5),  # ~40 km from anything
}


def test_route_follows_track_through_junction(graph):
    routes, report = route_all(graph, [pair(10, 1, 4, COORDS)])
    xy, flags = routes[10]
    assert flags == 0
    lons, lats = dequantize(xy)
    # A → D must pass through B: some point near (8.1, 47.0)
    near_b = np.min(np.hypot(lons - 8.1, lats - 47.0))
    assert near_b < 0.005
    assert report["fallback"] == {}


def test_unsnappable_station_falls_back_straight(graph):
    routes, report = route_all(graph, [pair(11, 1, 99, COORDS)])
    xy, flags = routes[11]
    assert flags == FLAG_STRAIGHT_FALLBACK
    assert len(xy) == 2
    assert report["fallback"] == {"unsnappable": 1}


def test_disconnected_component_falls_back(graph):
    routes, report = route_all(graph, [pair(12, 1, 5, COORDS)])
    _, flags = routes[12]
    assert flags == FLAG_STRAIGHT_FALLBACK
    assert report["fallback"] == {"unreachable": 1}


def test_parallel_network_snaps_to_common_component():
    """Two parallel lines meters apart: the pair must route on the network BOTH ends share,
    even when one end sits marginally closer to the other network (the Interlaken Ost bug)."""
    # Component A: long east-west line. Component B: parallel stub, slightly north, unconnected.
    lons = np.array([8.0, 8.1, 8.2, 8.0, 8.1])
    lats = np.array([47.0, 47.0, 47.0, 47.0006, 47.0006])
    edges = np.array([[0, 1], [1, 2], [3, 4]], dtype=np.int32)
    net = RailNetwork(node_ids=np.arange(5, dtype=np.int64), lons=lons, lats=lats, edges=edges)
    g = RailGraph.build(net)
    coords = {
        1: (8.0, 47.0004),  # nearer component B's node 3 than A's node 0
        2: (8.2, 47.0001),  # only component A is nearby
    }
    routes, report = route_all(g, [pair(20, 1, 2, coords)])
    xy, flags = routes[20]
    assert flags == 0 and report["fallback"] == {}
    _, lats_out = dequantize(xy)
    # Routed along component A (lat 47.0), not stranded on B
    assert np.all(lats_out < 47.0004)


def test_absurd_detour_falls_back_straight():
    """A pair whose only rail path is a huge detour (missing track in the extract) gets a
    straight line, not a train riding 60 km for a 1 km hop."""
    # A long U: west leg up, across, east leg down. Stations at the two bottom tips.
    lons = np.array([8.00, 8.00, 8.01, 8.01])
    lats = np.array([47.0, 47.3, 47.3, 47.0])
    edges = np.array([[0, 1], [1, 2], [2, 3]], dtype=np.int32)
    net = RailNetwork(node_ids=np.arange(4, dtype=np.int64), lons=lons, lats=lats, edges=edges)
    g = RailGraph.build(net)
    coords = {1: (8.0, 47.0001), 2: (8.01, 47.0001)}
    routes, report = route_all(g, [pair(30, 1, 2, coords)])
    xy, flags = routes[30]
    assert flags == FLAG_STRAIGHT_FALLBACK and len(xy) == 2
    assert report["fallback"] == {"absurd_detour": 1}


def test_endpoints_are_station_coords(graph):
    routes, _ = route_all(graph, [pair(13, 1, 3, COORDS)])
    lons, lats = dequantize(routes[13][0])
    # ~7 m quantization: endpoints match the station, not the snapped node
    assert abs(lons[0] - COORDS[1][0]) < 3e-4 and abs(lats[0] - COORDS[1][1]) < 3e-4
    assert abs(lons[-1] - COORDS[3][0]) < 3e-4 and abs(lats[-1] - COORDS[3][1]) < 3e-4


def test_simplify_drops_collinear_points():
    lons = np.linspace(8.0, 8.2, 101)  # 101 points on a perfectly straight line
    lats = np.full(101, 47.0)
    s_lons, s_lats = simplify_polyline(lons, lats)
    assert len(s_lons) == 2
    # and a genuine corner survives
    lons2 = np.array([8.0, 8.1, 8.1])
    lats2 = np.array([47.0, 47.0, 47.1])
    s2, _ = simplify_polyline(lons2, lats2)
    assert len(s2) == 3


def test_quantize_roundtrip_error_bounded():
    rng = np.random.default_rng(7)
    lons = rng.uniform(5.9, 10.5, 1000)
    lats = rng.uniform(45.8, 47.9, 1000)
    xy = quantize(lons, lats)
    q_lons, q_lats = dequantize(xy)
    ex, ey = to_meters(np.abs(lons - q_lons), np.abs(lats - q_lats))
    assert ex.max() < 4.0 and ey.max() < 2.0  # half a grid cell


def test_routes_bin_roundtrip(tmp_path: Path):
    routes = {
        7: (quantize(np.array([8.0, 8.1]), np.array([47.0, 47.0])), 0),
        3: (quantize(np.array([8.0, 8.05, 8.1]), np.array([47.0, 47.06, 47.1])), 1),
    }
    out = tmp_path / "routes.bin"
    stats = write_routes_bin(routes, out)
    assert stats["routes"] == 2 and stats["fallback_routes"] == 1

    rb = read_routes_bin(out)
    assert list(rb.route_ids) == [3, 7]  # ascending
    np.testing.assert_array_equal(rb.polyline(7), routes[7][0])
    np.testing.assert_array_equal(rb.polyline(3), routes[3][0])
    assert rb.flags[0] == 1 and rb.flags[1] == 0
    with pytest.raises(KeyError):
        rb.polyline(999)


def test_routes_bin_rejects_garbage(tmp_path: Path):
    bad = tmp_path / "bad.bin"
    bad.write_bytes(b"NOPE" + b"\x00" * 100)
    with pytest.raises(ValueError, match="magic"):
        read_routes_bin(bad)
