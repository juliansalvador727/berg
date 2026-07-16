import duckdb
import pytest

from berg_pipeline.constants import MAX_LEG_DURATION_S, SCHEMA_VERSION
from berg_pipeline.publish import R2_ENV_VARS, build_manifest, r2_from_env


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


def _clear_r2_env(monkeypatch):
    for var in (*R2_ENV_VARS, "BERG_REQUIRE_R2"):
        monkeypatch.delenv(var, raising=False)


def _set_r2_env(monkeypatch):
    for var in R2_ENV_VARS:
        monkeypatch.setenv(var, "x")


def test_r2_from_env_none_when_unconfigured(monkeypatch):
    _clear_r2_env(monkeypatch)
    assert r2_from_env() is None


def test_r2_from_env_builds_resource_when_configured(monkeypatch):
    _clear_r2_env(monkeypatch)
    _set_r2_env(monkeypatch)
    assert r2_from_env() is not None


def test_r2_from_env_raises_when_required_and_unset(monkeypatch):
    _clear_r2_env(monkeypatch)
    monkeypatch.setenv("BERG_REQUIRE_R2", "1")
    with pytest.raises(RuntimeError, match="R2_ACCOUNT_ID"):
        r2_from_env()


def test_r2_from_env_raises_when_required_and_one_secret_dropped(monkeypatch):
    """The CI failure this guards: three secrets present, one silently missing."""
    _clear_r2_env(monkeypatch)
    _set_r2_env(monkeypatch)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY")
    monkeypatch.setenv("BERG_REQUIRE_R2", "1")
    with pytest.raises(RuntimeError, match="R2_SECRET_ACCESS_KEY"):
        r2_from_env()


def test_r2_from_env_required_is_satisfied_when_configured(monkeypatch):
    _clear_r2_env(monkeypatch)
    _set_r2_env(monkeypatch)
    monkeypatch.setenv("BERG_REQUIRE_R2", "1")
    assert r2_from_env() is not None
