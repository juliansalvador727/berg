"""Contract values shared by the pipeline, the geometry job, and the frontend.

Anything here that changes is a breaking change to published data: bump SCHEMA_VERSION and
say so in manifest.json.
"""

SCHEMA_VERSION = 1

# The archive nominally starts in 2016, but 2016+2017 ship as a single ZIP named
# "unvollstaendig" — 166 MB for 24 months, against 397 MB for January 2018 alone. Usable
# coverage starts here. See docs/data-notes.md.
ARCHIVE_START = "2018-01-01"

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

# A *_PROGNOSE time is an observation only when its *_PROGNOSE_STATUS says so. In v2 that value
# is REAL: GESCHAETZT never appears for trains at all (only buses), despite the v1 lore.
# Verified against a full day — see docs/data-notes.md.
MEASURED_STATUS_V2 = "REAL"
MEASURED_STATUS_V1 = "GESCHAETZT"
