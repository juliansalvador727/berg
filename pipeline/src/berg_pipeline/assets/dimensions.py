"""Dimensions: stations (SCD2) and routes (geometry FK)."""

import json

import dagster as dg
from dagster_duckdb import DuckDBResource

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
def dim_route(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Registers the geometry job's routes.bin and checks it against the ingest registry.

    route_ids are assigned at ingest (the append-only station_pairs table); the geometry job
    consumes that registry and produces one polyline per id. This asset fails if published
    facts reference route_ids the geometry doesn't cover — that means the geometry job needs
    a re-run, which is expected whenever new months introduce new station pairs.
    """
    from berg_pipeline import routesbin

    routes_bin = paths.STATIC_DIR / "routes.bin"
    if not routes_bin.exists():
        raise dg.Failure(f"{routes_bin} missing — run geometry/ (see geometry/README.md)")
    header = routesbin.read_header(routes_bin)

    with duckdb.get_connection() as con:
        from berg_pipeline import ingest

        ingest.create_tables(con)
        registered = {r[0] for r in con.execute("SELECT route_id FROM station_pairs").fetchall()}
    missing = registered - set(header.route_ids)
    if missing:
        raise dg.Failure(
            f"{len(missing)} station pairs have no polyline in routes.bin "
            f"(e.g. route_ids {sorted(missing)[:5]}) — re-run the geometry job"
        )

    return dg.MaterializeResult(
        metadata={
            "routes": header.n_routes,
            "points": header.n_points,
            "fallback_routes": header.n_fallback,
            "bytes": routes_bin.stat().st_size,
            "orphan_polylines": len(set(header.route_ids) - registered),
        }
    )
