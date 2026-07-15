"""Rail network → routable graph: CSR adjacency for Dijkstra, KDTree for station snapping.

Deliberately not OSRM: its profiles are road-shaped and will happily route a train down a
street. We want the rail graph and nothing else.
"""

from dataclasses import dataclass

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree

from berg_geometry.pbf import RailNetwork
from berg_geometry.proj import to_meters

# Stations sit next to the track, not on it, so 'nearest' needs a ceiling — a station that
# snaps 300 m away is a bug (bus stop, closed line, bad coords), not a route. Its pairs fall
# back to straight lines rather than to somebody else's track.
SNAP_MAX_M = 300.0

# How many nearby nodes each station connects to. Needs to see PAST the nearest network at
# multi-gauge stations: at Interlaken Ost the metre-gauge track is meters closer than the
# standard-gauge one, and snapping to it alone routed West→Ost 187 km via Luzern.
SNAP_CANDIDATES = 32

# Snap edges cost distance × this. Two jobs: bias toward the closest sensible track when the
# rail paths are comparable, and make "cut through a station between two networks" cost a few
# km of virtual distance so it never beats staying on the right track. It cannot be huge —
# it must stay well under the pathological detours (90+ km) it exists to prevent.
SNAP_PENALTY = 10.0


@dataclass
class RailGraph:
    net: RailNetwork
    adj: csr_matrix  # symmetric, weights = meters
    tree: cKDTree  # over nodes in flat CH meters

    @classmethod
    def build(cls, net: RailNetwork) -> "RailGraph":
        x, y = to_meters(net.lons, net.lats)
        u, v = net.edges[:, 0], net.edges[:, 1]
        w = np.hypot(x[u] - x[v], y[u] - y[v])
        n = net.n_nodes
        adj = csr_matrix(
            (np.concatenate([w, w]), (np.concatenate([u, v]), np.concatenate([v, u]))),
            shape=(n, n),
        )
        return cls(net=net, adj=adj, tree=cKDTree(np.column_stack([x, y])))


@dataclass
class StationGraph:
    """The rail graph plus one virtual node per station, wired to all its nearby tracks.

    Choosing ONE snap node per station is wrong whichever way it's done: the nearest node
    picks a gauge at random at multi-network stations, and nearest-per-component doesn't help
    because the Swiss networks are physically connected somewhere and form one component.
    With a virtual node per station, Dijkstra minimizes snap + rail + snap per PAIR — the
    right network falls out of the shortest path itself.
    """

    adj: csr_matrix  # (n_rail + n_stations)²
    lons: np.ndarray  # rail nodes then station coords
    lats: np.ndarray
    n_rail: int
    vidx: dict[int, int]  # bpuic → virtual node index (absent: unsnappable)
    snap_dist: dict[int, float]  # bpuic → nearest-candidate distance, m

    @classmethod
    def build(cls, graph: RailGraph, stations: dict[int, tuple[float, float]]) -> "StationGraph":
        n = graph.net.n_nodes
        vidx: dict[int, int] = {}
        snap_dist: dict[int, float] = {}
        rows, cols, weights = [], [], []
        v_lons, v_lats = [], []
        for bpuic, (lon, lat) in sorted(stations.items()):
            x, y = to_meters(lon, lat)
            dists, idxs = graph.tree.query(
                [x, y], k=SNAP_CANDIDATES, distance_upper_bound=SNAP_MAX_M
            )
            found = [(float(d), int(i)) for d, i in zip(dists, idxs) if np.isfinite(d)]
            if not found:
                continue
            v = n + len(vidx)
            vidx[bpuic] = v
            snap_dist[bpuic] = found[0][0]
            v_lons.append(lon)
            v_lats.append(lat)
            for d, i in found:
                rows += [v, i]
                cols += [i, v]
                weights += [d * SNAP_PENALTY] * 2

        total = n + len(vidx)
        base = graph.adj.tocoo()
        adj = csr_matrix(
            (
                np.concatenate([base.data, np.asarray(weights)]),
                (
                    np.concatenate([base.row, np.asarray(rows, dtype=np.int64)]),
                    np.concatenate([base.col, np.asarray(cols, dtype=np.int64)]),
                ),
            ),
            shape=(total, total),
        )
        return cls(
            adj=adj,
            lons=np.concatenate([graph.net.lons, v_lons]),
            lats=np.concatenate([graph.net.lats, v_lats]),
            n_rail=n,
            vidx=vidx,
            snap_dist=snap_dist,
        )

    def paths_from(self, src: int, targets: list[int]) -> dict[int, np.ndarray | None]:
        """Shortest-path node sequences src → each target; None where unreachable.

        One Dijkstra per unique source (C-side), paths rebuilt from the predecessor array —
        the memory-light way to do ~1,700 sources over a ~million-node graph.
        """
        _, pred = dijkstra(self.adj, indices=src, return_predecessors=True)
        out: dict[int, np.ndarray | None] = {}
        for t in targets:
            if t == src:
                out[t] = None
                continue
            path = [t]
            while path[-1] != src:
                p = pred[path[-1]]
                if p < 0:
                    path = None
                    break
                path.append(p)
            out[t] = np.asarray(path[::-1], dtype=np.int64) if path is not None else None
        return out
