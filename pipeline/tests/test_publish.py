from datetime import date

import duckdb
import pytest

from berg_pipeline import paths, publish
from berg_pipeline.constants import MAX_LEG_DURATION_S, SCHEMA_VERSION
from berg_pipeline.publish import R2_ENV_VARS, build_manifest, r2_from_env


def _write_day(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"""
        COPY (SELECT 1::UINTEGER AS route_id, 0::USMALLINT AS journey_id,
                     1::UINTEGER AS t_dep, 1::USMALLINT AS dur, 1::UTINYINT AS type,
                     0::SMALLINT AS delay, 0::UTINYINT AS flags)
        TO '{path.as_posix()}' (FORMAT PARQUET)""")


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
    manifest = build_manifest(
        legs_dir, base_days=already_published, base_schema_version=SCHEMA_VERSION
    )

    assert manifest["start"] == "2018-05-01", "history must survive a one-month CI run"
    assert manifest["end"] == "2026-06-01"
    assert set(manifest["days"]) == {"2018-05-01", "2018-05-02", "2026-06-01"}
    assert manifest["days"]["2018-05-01"]["legs"] == 128535  # carried through untouched


def test_local_build_wins_over_the_published_entry(tmp_path):
    """A rebuilt day must replace what the bucket advertises, not be shadowed by it."""
    legs_dir = tmp_path / "legs"
    _write_day(legs_dir / "2018" / "05" / "01.parquet")
    manifest = build_manifest(
        legs_dir,
        base_days={"2018-05-01": {"bytes": 1, "legs": 999999}},
        base_schema_version=SCHEMA_VERSION,
    )
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


def test_manifest_drops_published_day_whose_remote_object_was_deleted(tmp_path):
    manifest = build_manifest(
        tmp_path / "legs",
        base_days={"2026-06-04": {"bytes": 100, "legs": 20}},
        base_schema_version=SCHEMA_VERSION,
        available_remote_keys=set(),
    )

    assert manifest["days"] == {}


def test_manifest_refuses_unrebuilt_history_from_an_older_schema(tmp_path):
    with pytest.raises(RuntimeError, match="rebuild the complete history"):
        build_manifest(
            tmp_path / "legs",
            base_days={"2018-05-01": {"bytes": 100, "legs": 20}},
            base_schema_version=SCHEMA_VERSION - 1,
            available_remote_keys={"legs/2018/05/01.parquet"},
        )


def test_manifest_refuses_old_local_leg_schema(tmp_path):
    legs_dir = tmp_path / "legs"
    old = legs_dir / "2018" / "05" / "01.parquet"
    old.parent.mkdir(parents=True)
    duckdb.sql(f"COPY (SELECT 1 AS route_id) TO '{old.as_posix()}' (FORMAT PARQUET)")

    with pytest.raises(RuntimeError, match="do not match schema"):
        build_manifest(legs_dir)


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


def test_validated_outputs_upload_as_one_post_validation_batch(tmp_path, monkeypatch):
    day = date(2026, 6, 3)
    monkeypatch.setattr(paths, "LEGS_DIR", tmp_path / "legs")
    monkeypatch.setattr(paths, "JOURNEYS_DIR", tmp_path / "journeys")
    monkeypatch.setattr(paths, "ROUTE_PAIRS_JSON", tmp_path / "static" / "route_pairs.json")
    monkeypatch.setattr(paths, "TRAIN_TYPES_JSON", tmp_path / "static" / "train_types.json")
    _write_day(paths.legs_parquet_path(day))
    _write_day(paths.journeys_parquet_path(day))
    paths.ROUTE_PAIRS_JSON.parent.mkdir(parents=True)
    paths.ROUTE_PAIRS_JSON.write_text("{}")
    paths.TRAIN_TYPES_JSON.write_text("{}")

    class FakeR2:
        def __init__(self):
            self.keys = []
            self.deleted = []

        def client(self):
            return self

        def upload(self, _path, key, client=None):
            assert client is self
            self.keys.append(key)

        def delete(self, key, client=None):
            assert client is self
            self.deleted.append(key)

    fake = FakeR2()
    monkeypatch.setattr(publish, "r2_from_env", lambda: fake)

    stats = publish.upload_validated_outputs([day])

    assert stats == {"uploaded": 4, "deleted": 0, "upload_enabled": True}
    assert fake.keys == [
        "legs/2026/06/03.parquet",
        "journeys/2026/06/03.parquet",
        "static/route_pairs.json",
        "static/train_types.json",
    ]
    assert fake.deleted == []


def test_validated_empty_day_deletes_stale_remote_outputs(tmp_path, monkeypatch):
    day = date(2026, 6, 4)
    monkeypatch.setattr(paths, "LEGS_DIR", tmp_path / "legs")
    monkeypatch.setattr(paths, "JOURNEYS_DIR", tmp_path / "journeys")
    monkeypatch.setattr(paths, "ROUTE_PAIRS_JSON", tmp_path / "missing-route-pairs.json")
    monkeypatch.setattr(paths, "TRAIN_TYPES_JSON", tmp_path / "missing-train-types.json")

    class FakeR2:
        def __init__(self):
            self.deleted = []

        def client(self):
            return self

        def upload(self, _path, _key, client=None):
            raise AssertionError("an empty day must not upload an artifact")

        def delete(self, key, client=None):
            assert client is self
            self.deleted.append(key)

    fake = FakeR2()
    monkeypatch.setattr(publish, "r2_from_env", lambda: fake)

    stats = publish.upload_validated_outputs([day])

    assert stats == {"uploaded": 0, "deleted": 2, "upload_enabled": True}
    assert fake.deleted == [
        "legs/2026/06/04.parquet",
        "journeys/2026/06/04.parquet",
    ]
