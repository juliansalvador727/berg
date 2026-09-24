"""Per-dataset configuration: the values the Swiss pipeline hardcodes as constants.

CH_BBOX, SOURCE_TZ, MIN_LEGS_PER_DAY and PUNCTUALITY_THRESHOLD_S are Swiss facts. A European
adapter supplies its own here instead; the Swiss constants stay untouched because the
published Swiss archive was built from them.
"""

from dataclasses import dataclass, field
from pathlib import Path

from berg_pipeline import paths


@dataclass(frozen=True)
class DatasetConfig:
    dataset_id: str
    country: str
    name: str
    timezone: str  # display timezone; source timestamps are converted to UTC by the adapter
    # Clip box for legs, lon/lat WGS84. routes.bin derives its own tighter quantization grid
    # from the stations actually served, so this only has to contain the network.
    bbox: tuple[float, float, float, float]
    time_semantics: str  # observed | final_prediction | delay_only | scheduled
    timestamp_precision_s: int
    scope: str
    provider: str
    license: str
    license_url: str
    attribution: str
    source_urls: tuple[str, ...]
    station_namespace: str
    # Official source convention, kept beside the normalized European threshold.
    punctuality_threshold_s: int
    # Smoke alarm for a collapsed ingest, an order of magnitude under a real day — the same
    # role MIN_LEGS_PER_DAY plays for Switzerland. Never a traffic band check.
    min_legs_per_day: int
    coverage_start: str
    coverage_end: str | None
    # europe.md's per-dataset publication cap. sync_dataset refuses an upload above it.
    storage_cap_bytes: int
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def root(self) -> Path:
        """Local working area: raw source, the dataset's DuckDB, and its publish mirror."""
        return paths.DATA_ROOT / "datasets" / self.dataset_id

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    @property
    def duckdb_path(self) -> Path:
        return self.root / "berg.duckdb"

    @property
    def dim_station_parquet(self) -> Path:
        return self.root / "dim_station.parquet"

    @property
    def publish_root(self) -> Path:
        """Byte-for-byte mirror of the bucket's datasets/<id>/ prefix."""
        return self.root / "publish"

    @property
    def key_prefix(self) -> str:
        return f"datasets/{self.dataset_id}/"


# A European comparison threshold, stated wherever a statistic uses it. Each dataset also
# keeps its official one.
EUROPEAN_PUNCTUALITY_THRESHOLD_S = 180

# europe.md's hard ceiling on projected steady-state R2 usage: 1 GB under the 10 GB free tier
# for GB-month averaging, temporary duplicates and estimation error. Never exceeded, not even
# temporarily.
R2_BUDGET_BYTES = 9_000_000_000

FINLAND = DatasetConfig(
    dataset_id="fi",
    country="FI",
    name="Finland",
    timezone="Europe/Helsinki",
    bbox=(19.0, 59.5, 32.0, 70.2),
    time_semantics="observed",
    timestamp_precision_s=1,
    scope="national-passenger",
    provider="Fintraffic / Digitraffic",
    license="CC BY 4.0",
    license_url="https://creativecommons.org/licenses/by/4.0/",
    attribution="Source: Fintraffic / digitraffic.fi, license CC 4.0 BY",
    source_urls=(
        "https://www.digitraffic.fi/en/railway-traffic/",
        "https://rata.digitraffic.fi/api/v1/trains/{date}",
        "https://rata.digitraffic.fi/api/v1/metadata/stations",
    ),
    station_namespace="fi-uic",
    # Fintraffic's published punctuality statistics count a train on time under 3 minutes for
    # commuter services and 5 minutes for long-distance ones. The wire carries delay in
    # seconds, so the normalized 3-minute threshold is what the UI applies.
    punctuality_threshold_s=180,
    # Measured 2023-01..02: 10.2k-14.6k published legs per UTC day, the low end on holidays.
    min_legs_per_day=2_000,
    coverage_start="2023-01-01",
    coverage_end="2025-12-31",
    storage_cap_bytes=150_000_000,
    notes=(
        "Actual times come from track-circuit observations; live estimates are never used.",
        "Only commercial stops are published; timing points a train passes are dropped.",
    ),
)

DATASETS: dict[str, DatasetConfig] = {FINLAND.dataset_id: FINLAND}
