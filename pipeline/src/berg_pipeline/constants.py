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

# A *_PROGNOSE time is an observation only when its *_PROGNOSE_STATUS says so. Two values mean
# "measured", and which one appears depends on the DATE, not on the file format version:
# GESCHAETZT until 2018-05-06, REAL from 2018-05-07. The v1→v2 format change (2025-07) is a
# different boundary entirely, and 2018-05-06 itself mixes both.
#
# So do NOT branch on era here — match the set. It is era-free, survives the mixed boundary day,
# and the two values are mutually exclusive per day everywhere else. GESCHAETZT does still occur
# post-2018 for BUSES, which the PRODUKT_ID='Zug' filter already removes.
# See docs/data-notes.md.
MEASURED_STATUSES = ("REAL", "GESCHAETZT")

# The day the archive switched enums. Nothing should branch on this — it is here so the
# provenance of MEASURED_STATUSES is not lost.
STATUS_ENUM_SWITCH = "2018-05-07"

# v2 launched MID-MONTH, on 2025-07-13. The v2 ZIP for 2025-07 therefore holds only days
# 13-31, while the v1 ZIP for the same month holds all 31 — and both series are published in
# parallel (v1 is not retired; verified through 2026-06). Switching series on "v2 exists yet"
# silently drops 12 real days and still reports the month a success.
#
# So switch at v2's first COMPLETE month and take v1 for the seam. Probed against every v2-era
# month's central directory: 2025-07 is the only one where the two series disagree.
# See docs/data-notes.md.
V2_FIRST_FULL_MONTH = (2025, 8)

# The archive's only schema change: SLOID was appended in 2025-11 — in BOTH the v1 and v2 URL
# series at once, four months after v2 launched. There is no "v1 schema" vs "v2 schema"; the
# first 21 columns are identical in name and order across 2018-01 → now.
#
# SLOID is unused here, so selecting the needed columns BY NAME spans the whole archive with no
# era view. Just never SELECT *. See docs/data-notes.md.
SLOID_APPEARS = "2025-11-01"

# The 21 columns present in every month of the archive, in file order.
COLUMNS_ALL_ERAS = (
    "BETRIEBSTAG",
    "FAHRT_BEZEICHNER",
    "BETREIBER_ID",
    "BETREIBER_ABK",
    "BETREIBER_NAME",
    "PRODUKT_ID",
    "LINIEN_ID",
    "LINIEN_TEXT",
    "UMLAUF_ID",
    "VERKEHRSMITTEL_TEXT",
    "ZUSATZFAHRT_TF",
    "FAELLT_AUS_TF",
    "BPUIC",
    "HALTESTELLEN_NAME",
    "ANKUNFTSZEIT",
    "AN_PROGNOSE",
    "AN_PROGNOSE_STATUS",
    "ABFAHRTSZEIT",
    "AB_PROGNOSE",
    "AB_PROGNOSE_STATUS",
    "DURCHFAHRT_TF",
)
