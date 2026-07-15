"""routes.bin — the polyline pack the frontend maps straight into typed arrays.

Struct-of-arrays, same philosophy as legs.bin: no parsing in the browser, just offsets.

    magic    b'BRTS'
    u32      version (=1)
    u32      n_routes
    f64 x 4  lon_min, lat_min, lon_max, lat_max   (the quantization grid)
    u32[n]   route_id           (ascending)
    u32[n+1] offsets            (point index; route i owns points offsets[i]..offsets[i+1])
    u8[n]    flags              (bit0 = straight-line fallback, no track found)
    u16[2p]  interleaved x,y    (quantized into the grid; ~7 m resolution across CH)

Little-endian throughout. Coordinates quantize into CH_BBOX — anything outside clamps, which
is fine because legs are already clipped to the bbox at ingest.
"""

import struct
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from berg_geometry.proj import CH_BBOX

MAGIC = b"BRTS"
VERSION = 1

FLAG_STRAIGHT_FALLBACK = 1 << 0


def quantize(lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
    """Degrees → interleaved uint16 grid coordinates, consecutive duplicates dropped."""
    lon0, lat0, lon1, lat1 = CH_BBOX
    x = np.clip(np.round((lons - lon0) / (lon1 - lon0) * 65535), 0, 65535)
    y = np.clip(np.round((lats - lat0) / (lat1 - lat0) * 65535), 0, 65535)
    xy = np.column_stack([x, y]).astype(np.uint16)
    if len(xy) > 1:
        keep = np.ones(len(xy), dtype=bool)
        keep[1:] = np.any(xy[1:] != xy[:-1], axis=1)
        xy = xy[keep]
    return xy


def dequantize(xy: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    lon0, lat0, lon1, lat1 = CH_BBOX
    return (
        xy[:, 0].astype(np.float64) / 65535 * (lon1 - lon0) + lon0,
        xy[:, 1].astype(np.float64) / 65535 * (lat1 - lat0) + lat0,
    )


@dataclass
class RoutesBin:
    route_ids: np.ndarray  # u32, ascending
    offsets: np.ndarray  # u32, len n+1
    flags: np.ndarray  # u8
    points: np.ndarray  # (p, 2) u16

    def polyline(self, route_id: int) -> np.ndarray:
        i = int(np.searchsorted(self.route_ids, route_id))
        if i >= len(self.route_ids) or self.route_ids[i] != route_id:
            raise KeyError(route_id)
        return self.points[self.offsets[i] : self.offsets[i + 1]]


def write_routes_bin(routes: dict[int, tuple[np.ndarray, int]], out_path: Path) -> dict:
    """routes: route_id → (interleaved-quantized points (p,2) u16, flags)."""
    ids = np.asarray(sorted(routes), dtype=np.uint32)
    counts = np.asarray([len(routes[i][0]) for i in ids], dtype=np.uint32)
    offsets = np.zeros(len(ids) + 1, dtype=np.uint32)
    np.cumsum(counts, out=offsets[1:])
    flags = np.asarray([routes[i][1] for i in ids], dtype=np.uint8)
    points = (
        np.concatenate([routes[i][0] for i in ids])
        if len(ids)
        else np.empty((0, 2), dtype=np.uint16)
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as fh:
        fh.write(MAGIC)
        fh.write(struct.pack("<II", VERSION, len(ids)))
        fh.write(struct.pack("<4d", *CH_BBOX))
        fh.write(ids.tobytes())
        fh.write(offsets.tobytes())
        fh.write(flags.tobytes())
        fh.write(points.astype("<u2").tobytes())

    return {
        "routes": len(ids),
        "points": int(offsets[-1]),
        "bytes": out_path.stat().st_size,
        "fallback_routes": int((flags & FLAG_STRAIGHT_FALLBACK).astype(bool).sum()),
    }


def read_routes_bin(path: Path) -> RoutesBin:
    raw = path.read_bytes()
    if raw[:4] != MAGIC:
        raise ValueError(f"{path}: not a routes.bin (magic {raw[:4]!r})")
    version, n = struct.unpack_from("<II", raw, 4)
    if version != VERSION:
        raise ValueError(f"{path}: version {version}, expected {VERSION}")
    bbox = struct.unpack_from("<4d", raw, 12)
    if tuple(round(v, 6) for v in bbox) != tuple(round(v, 6) for v in CH_BBOX):
        raise ValueError(f"{path}: bbox {bbox} != CH_BBOX {CH_BBOX}")
    o = 44
    route_ids = np.frombuffer(raw, dtype="<u4", count=n, offset=o)
    o += 4 * n
    offsets = np.frombuffer(raw, dtype="<u4", count=n + 1, offset=o)
    o += 4 * (n + 1)
    flags = np.frombuffer(raw, dtype="<u1", count=n, offset=o)
    o += n
    points = np.frombuffer(raw, dtype="<u2", offset=o).reshape(-1, 2)
    if len(points) != offsets[-1]:
        raise ValueError(f"{path}: {len(points)} points but offsets claim {offsets[-1]}")
    return RoutesBin(route_ids=route_ids, offsets=offsets, flags=flags, points=points)
