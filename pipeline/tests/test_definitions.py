"""The asset graph is only useful if it loads. This catches broken deps and typos before CI does."""

from berg_pipeline.definitions import defs
from berg_pipeline.partitions import daily_partitions


def test_definitions_load():
    keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert {"raw_zip", "stg_istdaten", "dim_station", "fct_legs", "legs_parquet"} <= keys
    assert daily_partitions.get_first_partition_key() == "2017-12-31"
