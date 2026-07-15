"""Minimal routes.bin reader — enough for the pipeline to validate coverage.

The format is defined and written by geometry/src/berg_geometry/binfmt.py (the authority);
this reader deliberately uses only the stdlib because the pipeline never touches polyline
geometry, just route ids and flags:

    magic b'BRTS' · u32 version · u32 n_routes · f64 bbox[4]
    u32 route_id[n] · u32 offsets[n+1] · u8 flags[n] · u16 xy[2 * points]
"""

import struct
from array import array
from dataclasses import dataclass
from pathlib import Path

MAGIC = b"BRTS"
VERSION = 1
FLAG_STRAIGHT_FALLBACK = 1 << 0


@dataclass
class RoutesHeader:
    n_routes: int
    bbox: tuple[float, float, float, float]
    route_ids: array  # u32
    flags: bytes
    n_points: int

    @property
    def n_fallback(self) -> int:
        return sum(1 for f in self.flags if f & FLAG_STRAIGHT_FALLBACK)


def read_header(path: Path) -> RoutesHeader:
    raw = path.read_bytes()
    if raw[:4] != MAGIC:
        raise ValueError(f"{path}: not a routes.bin (magic {raw[:4]!r})")
    version, n = struct.unpack_from("<II", raw, 4)
    if version != VERSION:
        raise ValueError(f"{path}: version {version}, expected {VERSION}")
    bbox = struct.unpack_from("<4d", raw, 12)
    o = 44
    route_ids = array("I")
    route_ids.frombytes(raw[o : o + 4 * n])
    o += 4 * n
    offsets = array("I")
    offsets.frombytes(raw[o : o + 4 * (n + 1)])
    o += 4 * (n + 1)
    flags = raw[o : o + n]
    return RoutesHeader(
        n_routes=n, bbox=bbox, route_ids=route_ids, flags=flags, n_points=offsets[-1]
    )
