import duckdb

from berg_pipeline.constants import MAX_LEG_DURATION_S, SCHEMA_VERSION
from berg_pipeline.publish import build_manifest, r2_from_env


def _write_day(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY (SELECT 1 AS x) TO '{path.as_posix()}' (FORMAT PARQUET)")


def test_build_manifest(tmp_path):
    legs_dir = tmp_path / "legs"
    _write_day(legs_dir / "2018" / "05" / "01.parquet")
    _write_day(legs_dir / "2018" / "05" / "03.parquet")

    manifest = build_manifest(legs_dir)

    assert manifest["schema_version"] == SCHEMA_VERSION
    assert manifest["max_leg_duration_s"] == MAX_LEG_DURATION_S
    assert manifest["start"] == "2018-05-01"
    assert manifest["end"] == "2018-05-03"
    assert manifest["missing_days"] == ["2018-05-02"]
    assert set(manifest["days"]) == {"2018-05-01", "2018-05-03"}
    for day in manifest["days"].values():
        assert day["bytes"] > 0
        assert day["legs"] == 1


def test_build_manifest_empty_dir(tmp_path):
    manifest = build_manifest(tmp_path / "legs")
    assert manifest["days"] == {}
    assert manifest["missing_days"] == []
    assert manifest["start"] is None
    assert manifest["end"] is None


def test_r2_from_env_none_when_unconfigured(monkeypatch):
    for var in ("R2_ACCOUNT_ID", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    assert r2_from_env() is None
