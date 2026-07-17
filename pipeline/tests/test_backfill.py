import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import backfill  # noqa: E402


def test_failed_month_does_not_publish_manifest(monkeypatch):
    monkeypatch.setattr(backfill, "acquire_lock", lambda: object())
    monkeypatch.setattr(backfill, "months_between", lambda _start, _end: ["2023-09"])

    def fail(_month, skip_dim):
        assert skip_dim
        raise RuntimeError("collapsed month")

    monkeypatch.setattr(backfill.build_month, "main", fail)
    published = []
    monkeypatch.setattr(backfill.dg, "materialize", lambda *a, **k: published.append((a, k)))

    assert backfill.main("2023-09", "2023-09", force=True) == 1
    assert published == []
