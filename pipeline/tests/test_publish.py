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


def test_ci_month_does_not_erase_published_history(tmp_path):
    """The monthly job checks out fresh and holds ONE month on disk.

    Without seeding from the bucket's manifest it would publish a manifest describing only
    that month — and since manifest.json is the frontend's only source of truth for what
    exists, the map would lose every other year.
    """
    legs_dir = tmp_path / "legs"
    _write_day(legs_dir / "2026" / "06" / "01.parquet")  # all CI has locally

    already_published = {
        "2018-05-01": {"bytes": 805012, "legs": 128535},
        "2018-05-02": {"bytes": 803000, "legs": 128000},
    }
    manifest = build_manifest(legs_dir, base_days=already_published)

    assert manifest["start"] == "2018-05-01", "history must survive a one-month CI run"
    assert manifest["end"] == "2026-06-01"
    assert set(manifest["days"]) == {"2018-05-01", "2018-05-02", "2026-06-01"}
    assert manifest["days"]["2018-05-01"]["legs"] == 128535  # carried through untouched


def test_local_build_wins_over_the_published_entry(tmp_path):
    """A rebuilt day must replace what the bucket advertises, not be shadowed by it."""
    legs_dir = tmp_path / "legs"
    _write_day(legs_dir / "2018" / "05" / "01.parquet")
    manifest = build_manifest(legs_dir, base_days={"2018-05-01": {"bytes": 1, "legs": 999999}})
    assert manifest["days"]["2018-05-01"]["legs"] == 1  # the fixture day has one row
    assert manifest["days"]["2018-05-01"]["bytes"] > 1


def test_manifest_keys_are_the_published_contract(tmp_path):
    """The frontend types these by hand (web/src/types.ts). Change one, change both."""
    legs_dir = tmp_path / "legs"
    _write_day(legs_dir / "2018" / "05" / "01.parquet")
    assert set(build_manifest(legs_dir)) == {
        "schema_version",
        "max_leg_duration_s",
        "generated_at",
        "start",
        "end",
        "days",
        "missing_days",
    }


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
