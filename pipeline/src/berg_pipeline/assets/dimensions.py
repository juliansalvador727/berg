"""Dimensions: stations (SCD2) and routes (geometry FK)."""

import json

import dagster as dg

from berg_pipeline import paths
from berg_pipeline.constants import CH_BBOX
from berg_pipeline.dim_station import build_full


@dg.asset(group_name="dimensions")
def dim_station() -> dg.MaterializeResult:
    """One row per (bpuic, validity range), diffed from archived GTFS stops.txt snapshots.

    Type 2 on purpose: a 2018 stop event must join to the 2018 name and coordinates, not
    today's. Every fact join is
        ON f.bpuic = d.bpuic AND f.service_day BETWEEN d.valid_from AND d.valid_to
    Unmatched BPUICs are quarantined and counted, never silently dropped.

    Fetches are cached under data/gtfs, so a re-materialization only downloads snapshots
    published since the last run (~130 MB ZIPs, ~2.6 MB stops.txt each, by range request).
    """
    stats = build_full(paths.GTFS_CACHE, paths.DIM_STATION_PARQUET)
    return dg.MaterializeResult(metadata=stats)


@dg.asset(group_name="dimensions", deps=[dim_station])
def stations_json() -> dg.MaterializeResult:
    """Flattened current-snapshot station list for the frontend bundle (id, name, lon, lat)."""
    import duckdb

    lon_min, lat_min, lon_max, lat_max = CH_BBOX
    rows = duckdb.sql(f"""
        SELECT bpuic, name, lon, lat
        FROM '{paths.DIM_STATION_PARQUET.as_posix()}'
        WHERE valid_to = DATE '9999-12-31'
          AND lon BETWEEN {lon_min} AND {lon_max}
          AND lat BETWEEN {lat_min} AND {lat_max}
        ORDER BY bpuic""").fetchall()

    out = paths.STATIC_DIR / "stations.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps([{"id": r[0], "name": r[1], "lon": r[2], "lat": r[3]} for r in rows]))
    return dg.MaterializeResult(metadata={"stations": len(rows), "bytes": out.stat().st_size})


@dg.asset(group_name="dimensions")
def dim_route() -> dg.MaterializeResult:
    """Lookup from (from_bpuic, to_bpuic) → route_id, produced by the geometry/ job.

    The ingest side of this already exists: fct_legs assigns stable route_ids via the
    station_pairs registry. This asset is the geometry half — polylines for each pair —
    and only registers the geometry job's output; see geometry/README.md.
    """
    raise NotImplementedError("M2: load routes.bin manifest produced by geometry/")
