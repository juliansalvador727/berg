"""One flat projection for everything metric in this job.

Switzerland spans ~3.5° of longitude, small enough for an equirectangular projection around
its center latitude: errors stay well under 0.5%, which is nothing against a 10 m simplify
tolerance and a 300 m snap ceiling. Using the same projection for edge weights, snapping,
and simplification keeps all the tolerances in one unit (meters) with no library dependency.
"""

import numpy as np

# Must match berg_pipeline.constants.CH_BBOX — the wire quantization grid.
CH_BBOX = (5.9, 45.8, 10.5, 47.9)

M_PER_DEG_LAT = 111_132.0
M_PER_DEG_LON = 111_320.0 * float(np.cos(np.deg2rad((CH_BBOX[1] + CH_BBOX[3]) / 2)))


def set_reference_latitude(lat: float) -> None:
    """Re-center the projection for another network, before anything is projected.

    Switzerland needs one factor; a taller network does not fit it. Finland spans ~60-67.5°N,
    where cos(lat) runs 0.50 → 0.38, so a single factor at the center is off by up to ~14%
    east-west at the extremes. That is still far inside what the metric tolerances care about
    (a 300 m snap ceiling, a 10 m simplify, a 4× detour ratio), and it keeps one unit
    everywhere. Build a network much taller than that and this needs a real projection.
    """
    global M_PER_DEG_LON
    M_PER_DEG_LON = 111_320.0 * float(np.cos(np.deg2rad(lat)))


def to_meters(lon, lat):
    """Degrees → flat CH meters. Works elementwise on scalars or numpy arrays."""
    return lon * M_PER_DEG_LON, lat * M_PER_DEG_LAT


def seg_lengths_m(lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
    """Length of each consecutive segment of a polyline, in meters."""
    x, y = to_meters(lons, lats)
    return np.hypot(np.diff(x), np.diff(y))
