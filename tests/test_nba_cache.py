from __future__ import annotations

import json
from pathlib import Path

import pytest
from requests import RequestException

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


def test_load_or_fetch_league_games_retries_and_caches(tmp_path: Path, monkeypatch):
    calls: list[int] = []
    sleeps: list[float] = []

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            calls.append(1)
            if len(calls) < 3:
                raise RequestException("temporary failure")

        def get_dict(self):
            return {"resultSets": [{"headers": ["GAME_ID"], "rowSet": [["0001"]]}]}

    monkeypatch.setattr(
        "wowy.nba_cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr("wowy.nba_cache.time.sleep", sleeps.append)

    from wowy.nba_cache import load_or_fetch_league_games

    payload = load_or_fetch_league_games(
        team_id=1610612738,
        team_abbreviation="BOS",
        season="2023-24",
        season_type="Regular Season",
        source_data_dir=tmp_path,
    )

    assert payload["resultSets"][0]["rowSet"] == [["0001"]]
    assert len(calls) == 3
    assert sleeps == [2.0, 4.0]

    cached_payload = load_or_fetch_league_games(
        team_id=1610612738,
        team_abbreviation="BOS",
        season="2023-24",
        season_type="Regular Season",
        source_data_dir=tmp_path,
    )

    assert cached_payload == payload
    assert len(calls) == 3
