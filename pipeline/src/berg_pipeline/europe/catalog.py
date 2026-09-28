"""Dataset manifests and the root catalog.json that lists every dataset.

The catalog sits ABOVE the Swiss archive rather than beside it: Switzerland keeps its root
manifest.json, schema v3, and every object key, and old clients that never read the catalog
keep working. A dataset's `path` is relative to the bucket root; "" is the Swiss root.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from berg_pipeline import publish
from berg_pipeline.constants import CH_BBOX, PUNCTUALITY_THRESHOLD_S, SCHEMA_VERSION
from berg_pipeline.europe.config import (
    DATASETS,
    EUROPEAN_PUNCTUALITY_THRESHOLD_S,
    DatasetConfig,
)

CATALOG_SCHEMA_VERSION = 1
CATALOG_KEY = "catalog.json"

# Written by hand, never regenerated from Swiss data: the point of the catalog is that
# Switzerland needs no rebuild to join it.
SWITZERLAND_ENTRY = {
    "path": "",
    "name": "Switzerland",
    "country": "CH",
    "timezone": "Europe/Zurich",
    "bbox": list(CH_BBOX),
    "leg_schema_version": SCHEMA_VERSION,
    "time_semantics": "observed",
    "timestamp_precision_s": 60,
    "scope": "national-passenger",
    "provider": "opentransportdata.swiss (Ist-Daten)",
    "license": "opentransportdata.swiss terms of use",
    "license_url": "https://opentransportdata.swiss/en/terms-of-use/",
    "attribution": "Source: opentransportdata.swiss",
    "station_namespace": "ch-bpuic",
    "punctuality_threshold_s": PUNCTUALITY_THRESHOLD_S,
    "coverage": [{"start": "2018-01-01", "end": None}],
}


def dataset_metadata(cfg: DatasetConfig) -> dict:
    """The provenance block shared by a dataset's manifest and its catalog entry."""
    return {
        "dataset_id": cfg.dataset_id,
        "name": cfg.name,
        "country": cfg.country,
        "timezone": cfg.timezone,
        "bbox": list(cfg.bbox),
        "leg_schema_version": SCHEMA_VERSION,
        "time_semantics": cfg.time_semantics,
        "timestamp_precision_s": cfg.timestamp_precision_s,
        "scope": cfg.scope,
        "provider": cfg.provider,
        "license": cfg.license,
        "license_url": cfg.license_url,
        "attribution": cfg.attribution,
        "source_urls": list(cfg.source_urls),
        "station_namespace": cfg.station_namespace,
        "punctuality_threshold_s": cfg.punctuality_threshold_s,
        "european_punctuality_threshold_s": EUROPEAN_PUNCTUALITY_THRESHOLD_S,
        "coverage": [{"start": cfg.coverage_start, "end": cfg.coverage_end}],
        "capabilities": {"cancellations": False, "added_journeys": False},
        "notes": list(cfg.notes),
    }


def build_dataset_manifest(cfg: DatasetConfig, extra: dict | None = None) -> dict:
    """The ordinary leg manifest (same fields the Swiss client reads) plus provenance.

    Coverage is the configured window, and every UTC day inside it without a file is listed
    in missing_days — so a gap is advertised as missing coverage, never as a day without
    trains.
    """
    legs_dir = cfg.publish_root / "legs"
    manifest = publish.build_manifest(legs_dir)
    days = manifest["days"]
    outside = [d for d in days if not cfg.coverage_start <= d <= (cfg.coverage_end or "9999")]
    if outside:
        raise RuntimeError(f"{cfg.dataset_id}: day files outside coverage: {outside[:3]}")
    manifest["start"] = cfg.coverage_start
    manifest["end"] = cfg.coverage_end or manifest["end"]
    manifest["missing_days"] = _missing(cfg.coverage_start, manifest["end"], days)
    manifest |= dataset_metadata(cfg)
    quality = cfg.root / "quality.json"
    if quality.exists():
        report = json.loads(quality.read_text())
        manifest["source_cancelled_days"] = [
            d["day"] for d in report.get("source_cancelled_days", [])
        ]
        # UTC hours inside published days that the source itself is missing (a crawl hole):
        # the client must show them as a gap, never as an hour without trains.
        if report.get("source_gap_hours"):
            manifest["source_gap_hours"] = report["source_gap_hours"]
        if report.get("source_revision"):
            manifest["source_revision"] = report["source_revision"]
    if extra:
        manifest |= extra
    return manifest


def _missing(start: str, end: str, days: dict) -> list[str]:
    from datetime import date, timedelta

    out, d, last = [], date.fromisoformat(start), date.fromisoformat(end)
    while d <= last:
        if d.isoformat() not in days:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def build_catalog(
    published: dict[str, dict], existing: dict | None = None, links: dict | None = None
) -> dict:
    """published: dataset_id → its manifest, for the dataset(s) this sync just uploaded.

    Entries already in the live catalog for other datasets are carried over untouched. A
    dataset enters the catalog only once its manifest is live — the catalog is the last commit
    point, exactly as manifest.json is for one dataset.
    """
    carried = dict((existing or {}).get("datasets", {}))
    datasets = {"ch": SWITZERLAND_ENTRY}
    for dataset_id, entry in sorted(carried.items()):
        if dataset_id != "ch" and dataset_id not in published:
            datasets[dataset_id] = entry
    for dataset_id, manifest in sorted(published.items()):
        cfg = DATASETS[dataset_id]
        entry = {"path": cfg.key_prefix.rstrip("/")} | dataset_metadata(cfg)
        entry.pop("dataset_id")
        entry["coverage"] = [{"start": manifest["start"], "end": manifest["end"]}]
        datasets[dataset_id] = entry
    out = {
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "datasets": datasets,
    }
    # The cross-border layer (europe/links.py) is carried over like any dataset entry, and
    # replaced only by the sync that publishes it. Clients without it draw every dataset alone.
    links = links or (existing or {}).get("links")
    if links:
        out["links"] = links
    return out


def write_json(obj: dict, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(obj, indent=1))
