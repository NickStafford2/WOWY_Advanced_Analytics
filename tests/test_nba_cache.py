from __future__ import annotations

import json
from pathlib import Path

from wowy.nba_cache import load_cached_payload, write_cached_payload


def test_write_cached_payload_writes_json_atomically(tmp_path: Path):
    cache_path = tmp_path / "cache" / "payload.json"

    write_cached_payload(cache_path, {"value": 1})

    assert cache_path.exists()
    assert json.loads(cache_path.read_text(encoding="utf-8")) == {"value": 1}
    assert not list(cache_path.parent.glob("*.tmp-*"))


def test_load_cached_payload_ignores_corrupt_json(tmp_path: Path):
    cache_path = tmp_path / "cache" / "payload.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text('{"value": ', encoding="utf-8")

    assert load_cached_payload(cache_path) is None
