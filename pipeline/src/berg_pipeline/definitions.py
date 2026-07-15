import dagster as dg

from berg_pipeline import assets
from berg_pipeline.resources import default_duckdb, default_r2

defs = dg.Definitions(
    assets=dg.load_assets_from_modules(
        [assets.raw, assets.dimensions, assets.legs, assets.aggregates]
    ),
    asset_checks=dg.load_asset_checks_from_modules(
        [assets.raw, assets.dimensions, assets.legs, assets.aggregates]
    ),
    resources={
        "duckdb": default_duckdb(),
        "r2": default_r2(),
    },
)
