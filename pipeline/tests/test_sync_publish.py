import importlib.util
import json
import struct
from pathlib import Path

import duckdb

from berg_pipeline import paths

SYNC_SCRIPT = Path(__file__).parents[1] / "scripts" / "sync_publish.py"
SYNC_SPEC = importlib.util.spec_from_file_location("sync_publish", SYNC_SCRIPT)
assert SYNC_SPEC is not None and SYNC_SPEC.loader is not None
sync_publish = importlib.util.module_from_spec(SYNC_SPEC)
SYNC_SPEC.loader.exec_module(sync_publish)


def _write_leg_day(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"""
        COPY (SELECT 1::UINTEGER AS route_id, 0::USMALLINT AS journey_id,
                     1::UINTEGER AS t_dep, 1::USMALLINT AS dur, 1::UTINYINT AS type,
                     0::SMALLINT AS delay, 0::UTINYINT AS flags)
        TO '{path.as_posix()}' (FORMAT PARQUET)""")


def _write_routes(path: Path, route_ids: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = len(route_ids)
    path.write_bytes(
        struct.pack("<4sII4d", b"BRTS", 1, n, 5.9, 45.8, 10.5, 47.9)
        + struct.pack(f"<{n}I", *route_ids)
        + struct.pack(f"<{n + 1}I", *([0] * (n + 1)))
        + bytes(n)
    )


def _mirror(tmp_path: Path, monkeypatch) -> Path:
    root = tmp_path / "publish"
    legs = root / "legs"
    monkeypatch.setattr(paths, "PUBLISH_ROOT", root)
    monkeypatch.setattr(paths, "LEGS_DIR", legs)
    monkeypatch.setattr(paths, "JOURNEYS_DIR", root / "journeys")
    monkeypatch.setattr(paths, "STATIC_DIR", root / "static")
    monkeypatch.setattr(paths, "ROUTE_PAIRS_JSON", root / "static" / "route_pairs.json")

    _write_leg_day(legs / "2026" / "06" / "03.parquet")
    journey = paths.JOURNEYS_DIR / "2026" / "06" / "03.parquet"
    journey.parent.mkdir(parents=True)
    journey.write_bytes(b"journey")
    _write_routes(paths.STATIC_DIR / "routes.bin", [0])
    paths.ROUTE_PAIRS_JSON.write_text(json.dumps({"0": [8500001, 8500002]}))
    (paths.STATIC_DIR / "stations.json").write_text("[]")
    (paths.STATIC_DIR / "train_types.json").write_text("{}")
    return root


class FakeR2:
    bucket = "berg-test"

    def __init__(self, existing: dict[str, dict] | None = None):
        self.existing = existing or {}
        self.events: list[tuple[str, str]] = []

    def client(self):
        return self

    def existing_objects(self, client=None):
        assert client is self
        return self.existing

    def object_matches(self, _path, _key, _remote, client=None):
        assert client is self
        return False

    def upload(self, _path, key, client=None):
        assert client is self
        self.events.append(("upload", key))

    def delete(self, key, client=None):
        assert client is self
        self.events.append(("delete", key))


def test_upload_order_is_geometry_safe():
    keys = [
        "legs/2026/06/03.parquet",
        "other/report.json",
        "journeys/2026/06/03.parquet",
        "static/routes.bin",
    ]

    assert sorted(keys, key=sync_publish.upload_order) == [
        "static/routes.bin",
        "journeys/2026/06/03.parquet",
        "legs/2026/06/03.parquet",
        "other/report.json",
    ]


def test_stale_keys_are_limited_to_managed_prefixes():
    remote = {
        "legs/2019/07/02.parquet",
        "journeys/2019/07/02.parquet",
        "static/old.json",
        "tiles/switzerland.pmtiles",
        "unrelated.txt",
    }

    assert sync_publish.managed_stale_keys(remote, set()) == [
        "journeys/2019/07/02.parquet",
        "legs/2019/07/02.parquet",
        "static/old.json",
    ]


def test_preflight_rejects_stale_geometry(tmp_path, monkeypatch):
    _mirror(tmp_path, monkeypatch)
    paths.ROUTE_PAIRS_JSON.write_text(
        json.dumps({"0": [8500001, 8500002], "1": [8500002, 8500003]})
    )

    try:
        sync_publish.validate_local_mirror()
    except RuntimeError as error:
        assert "1 registered route ids have no geometry" in str(error)
    else:
        raise AssertionError("stale geometry must block the sync")


def test_preflight_rejects_leg_journey_asymmetry(tmp_path, monkeypatch):
    _mirror(tmp_path, monkeypatch)
    (paths.JOURNEYS_DIR / "2026" / "06" / "03.parquet").unlink()

    try:
        sync_publish.validate_local_mirror()
    except RuntimeError as error:
        assert "1 legs-only" in str(error)
    else:
        raise AssertionError("a missing journey sidecar must block the sync")


def test_dry_run_is_remote_aware_and_writes_nothing(tmp_path, monkeypatch, capsys):
    root = _mirror(tmp_path, monkeypatch)
    fake = FakeR2(
        {
            "legs/2019/07/02.parquet": {"size": 1, "etag": "x"},
            "unrelated.txt": {"size": 1, "etag": "x"},
        }
    )
    monkeypatch.setattr(sync_publish.publish, "r2_from_env", lambda: fake)

    assert sync_publish.main(dry_run=True, delete=True) == 0

    assert not (root / sync_publish.MANIFEST_KEY).exists()
    assert fake.events == []
    output = capsys.readouterr().out
    assert output.index("static: 4 to upload") < output.index("journeys: 1 to upload")
    assert output.index("journeys: 1 to upload") < output.index("legs: 1 to upload")
    assert "after manifest: 1 managed objects to delete" in output
    assert "unrelated.txt" not in output


def test_real_sync_commits_manifest_before_managed_deletes(tmp_path, monkeypatch):
    root = _mirror(tmp_path, monkeypatch)
    fake = FakeR2(
        {
            "journeys/2019/07/02.parquet": {"size": 1, "etag": "x"},
            "legs/2019/07/02.parquet": {"size": 1, "etag": "x"},
            "static/old.json": {"size": 1, "etag": "x"},
            "unrelated.txt": {"size": 1, "etag": "x"},
        }
    )
    monkeypatch.setattr(sync_publish.publish, "r2_from_env", lambda: fake)

    assert sync_publish.main(dry_run=False, delete=True) == 0

    assert (root / sync_publish.MANIFEST_KEY).exists()
    assert fake.events == [
        ("upload", "static/route_pairs.json"),
        ("upload", "static/routes.bin"),
        ("upload", "static/stations.json"),
        ("upload", "static/train_types.json"),
        ("upload", "journeys/2026/06/03.parquet"),
        ("upload", "legs/2026/06/03.parquet"),
        ("upload", "manifest.json"),
        ("delete", "journeys/2019/07/02.parquet"),
        ("delete", "legs/2019/07/02.parquet"),
        ("delete", "static/old.json"),
    ]


def test_delete_is_opt_in(tmp_path, monkeypatch):
    _mirror(tmp_path, monkeypatch)
    fake = FakeR2({"legs/2019/07/02.parquet": {"size": 1, "etag": "x"}})
    monkeypatch.setattr(sync_publish.publish, "r2_from_env", lambda: fake)

    assert sync_publish.main(dry_run=False, delete=False) == 0

    assert all(action != "delete" for action, _key in fake.events)
