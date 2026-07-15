"""Contract values shared by the pipeline, the geometry job, and the frontend.

Anything here that changes is a breaking change to published data: bump SCHEMA_VERSION and
say so in manifest.json.
"""

SCHEMA_VERSION = 1

# The archive starts here; there is no Ist-Daten before 2016.
ARCHIVE_START = "2016-01-01"

# Legs longer than this are split into synthetic sub-legs at polyline vertices (flags bit1).
# This is what bounds the scrub query: every leg in flight at T departed in [T-3600, T].
# The frontend hardcodes nothing — it reads this from manifest.json.
MAX_LEG_DURATION_S = 3600

# Ist-Daten is Europe/Zurich local time. Convert to UTC at ingest, render local in the UI.
SOURCE_TZ = "Europe/Zurich"

# Switzerland bounding box (lon/lat, WGS84). Used to clip foreign stops and to quantize
# route coordinates into uint16.
CH_BBOX = (5.9, 45.8, 10.5, 47.9)

# Swiss punctuality convention: a train is on time if it is less than 3 minutes late.
PUNCTUALITY_THRESHOLD_S = 180

# Leg fact bit flags.
FLAG_SCHEDULED_FALLBACK = 1 << 0  # time is scheduled, not measured
FLAG_SYNTHETIC_SPLIT = 1 << 1  # produced by the MAX_LEG_DURATION_S split rule
