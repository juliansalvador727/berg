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
    # Station countries whose legs are published; a leg with an endpoint elsewhere is clipped
    # as outside_country. None keeps only the bbox clip. A country's box is a poor border:
    # the Dutch one also holds Antwerp, Aachen and Cologne.
    countries: tuple[str, ...] | None = None
    # zstd level for day files; None is DuckDB's default. Only the encoder slows down with the
    # level, and zstd decodes at the same speed, so a dataset over its cap raises it before
    # anything else gives (europe.md's remedy 3).
    compression_level: int | None = None

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

NETHERLANDS = DatasetConfig(
    dataset_id="nl",
    country="NL",
    name="Netherlands",
    timezone="Europe/Amsterdam",
    bbox=(3.2, 50.7, 7.3, 53.6),
    # The archive publishes scheduled times plus the last known delay in whole minutes from
    # NS's realtime feed — never an absolute actual time, and a zero delay cannot be told
    # apart from a train that never reported. See docs/data-notes-nl.md.
    time_semantics="delay_only",
    timestamp_precision_s=60,
    scope="national-passenger",
    provider="Rijden de Treinen",
    license="CC BY 4.0",
    license_url="https://creativecommons.org/licenses/by/4.0/",
    attribution="Source: Rijden de Treinen (rijdendetreinen.nl), CC BY 4.0",
    source_urls=(
        "https://www.rijdendetreinen.nl/en/open-data/train-archive",
        "https://opendata.rijdendetreinen.nl/public/services/services-{YYYY-MM}.csv.gz",
        "https://opendata.rijdendetreinen.nl/public/stations/stations-2023-09.csv",
    ),
    station_namespace="uic",
    # The wire carries delay in seconds from whole-minute source values; the normalized
    # 3-minute European threshold is what the UI applies.
    punctuality_threshold_s=180,
    # Measured 2023-2025: roughly 3.5k-7k services per service day.
    min_legs_per_day=5_000,
    coverage_start="2023-01-01",
    coverage_end="2025-12-31",
    storage_cap_bytes=500_000_000,
    countries=("NL",),
    notes=(
        "Times are scheduled plus the last known delay in whole minutes; not observations.",
        "Only legs between Dutch stations are published; cross-border legs are clipped.",
        "Bus, metro and tram replacement services are excluded.",
    ),
)

BELGIUM = DatasetConfig(
    dataset_id="be",
    country="BE",
    name="Belgium",
    timezone="Europe/Brussels",
    bbox=(2.5, 49.45, 6.45, 51.55),
    # Planned and actual times to the second from Infrabel's own train detection.
    time_semantics="observed",
    timestamp_precision_s=1,
    scope="national-passenger",
    provider="Infrabel",
    license="CC0 1.0",
    license_url="https://creativecommons.org/publicdomain/zero/1.0/",
    attribution="Source: Infrabel (opendata.infrabel.be), CC0",
    source_urls=(
        "https://opendata.infrabel.be/explore/dataset/stiptheid-gegevens-maandelijksebestanden/",
        "https://fr.ftp.opendatasoft.com/infrabel/PunctualityHistory/Data_raw_punctuality_{YYYYMM}.csv",
        "https://opendata.infrabel.be/explore/dataset/operationele-punten-van-het-netwerk/",
    ),
    station_namespace="be-ptcar",
    # Infrabel counts a train on time when it is less than 6 minutes late.
    punctuality_threshold_s=360,
    # Measured 2023-2025: 38-44k legs per weekday, ~22k per weekend, and 7.2k on the
    # 2025-01-13 national rail strike, the thinnest day. The source lists only trains that
    # ran, so a strike cannot be excused as source-cancelled; the floor sits under it.
    min_legs_per_day=5_000,
    coverage_start="2023-01-01",
    coverage_end="2025-12-31",
    storage_cap_bytes=350_000_000,
    countries=("BE",),
    # Second-precision times on 35k short hops a day: level 19 is 10% smaller than the
    # default on a measured weekday, and no higher level helps.
    compression_level=19,
    notes=(
        "Actual times come from Infrabel's train detection; only trains that ran are listed.",
        "Only Infrabel's network is covered: international trains end at their last Belgian stop.",
        "Cancelled trains and stops are absent from the source, not flagged.",
    ),
)

DATASETS: dict[str, DatasetConfig] = {d.dataset_id: d for d in (FINLAND, NETHERLANDS, BELGIUM)}
