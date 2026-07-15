"""The pipeline-side routes.bin reader against a file built byte-by-byte.

If geometry/binfmt.py (the writer, the format authority) changes layout, this fixture is the
cross-project contract that must break loudly.
"""

import struct
from pathlib import Path

import pytest

from berg_pipeline import routesbin

CH_BBOX = (5.9, 45.8, 10.5, 47.9)


def make_routes_bin(path: Path) -> None:
    # Two routes: id 3 (2 points, fallback flag) and id 7 (3 points, routed).
    path.write_bytes(
        b"BRTS"
        + struct.pack("<II", 1, 2)
        + struct.pack("<4d", *CH_BBOX)
        + struct.pack("<2I", 3, 7)  # route_ids
        + struct.pack("<3I", 0, 2, 5)  # offsets
        + bytes([1, 0])  # flags
        + struct.pack("<10H", *range(10))  # 5 points
    )


def test_read_header(tmp_path: Path):
    f = tmp_path / "routes.bin"
    make_routes_bin(f)
    h = routesbin.read_header(f)
    assert h.n_routes == 2
    assert list(h.route_ids) == [3, 7]
    assert h.n_points == 5
    assert h.n_fallback == 1
    assert h.bbox == CH_BBOX


def test_rejects_wrong_magic(tmp_path: Path):
    f = tmp_path / "bad.bin"
    f.write_bytes(b"NOPE" + b"\x00" * 60)
    with pytest.raises(ValueError, match="magic"):
        routesbin.read_header(f)


def test_rejects_wrong_version(tmp_path: Path):
    f = tmp_path / "v9.bin"
    f.write_bytes(b"BRTS" + struct.pack("<II", 9, 0) + struct.pack("<4d", *CH_BBOX))
    with pytest.raises(ValueError, match="version"):
        routesbin.read_header(f)
