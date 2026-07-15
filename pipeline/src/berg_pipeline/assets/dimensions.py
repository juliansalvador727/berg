"""Dimensions: stations (SCD2) and routes (geometry FK)."""

import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset(group_name="dimensions")
def dim_station(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """One row per (bpuic, validity range), from the DIDOK 'Alle Versionen' export.

    Type 2 on purpose: a 2017 stop event must join to the 2017 name and coordinates, not
    today's. Every fact join is
        ON f.bpuic = d.bpuic AND f.date BETWEEN d.valid_from AND d.valid_to
    Unmatched BPUICs are quarantined and counted, never silently dropped.
    """
    raise NotImplementedError("M1: build dim_station from DIDOK all-versions export")


@dg.asset(group_name="dimensions", deps=[dim_station])
def stations_json(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Flattened current-snapshot station list for the frontend bundle (id, name, lon, lat)."""
    raise NotImplementedError("M1: emit static/stations.json")


@dg.asset(group_name="dimensions")
def dim_route() -> dg.MaterializeResult:
    """Lookup from (from_bpuic, to_bpuic) → route_id, produced by the geometry/ job.

    Slow and rare: the OSM rail graph changes far less often than the timetable. This asset
    only registers the geometry job's output; see geometry/README.md to regenerate it.
    """
    raise NotImplementedError("M2: load routes.bin manifest produced by geometry/")
