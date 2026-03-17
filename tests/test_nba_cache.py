from __future__ import annotations

import json
from pathlib import Path

import pytest
from requests import RequestException

from wowy.nba.cache import (
    BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
    LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS,
    load_cached_payload,
    load_or_fetch_box_score_with_source,
    load_or_fetch_league_games_with_source,
    write_cached_payload,
)


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
            assert kwargs["timeout"] == LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS
            calls.append(1)
            if len(calls) < 3:
                raise RequestException("temporary failure")

        def get_dict(self):
            return {"resultSets": [{"headers": ["GAME_ID"], "rowSet": [["0001"]]}]}

    monkeypatch.setattr(
        "wowy.nba.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr("wowy.nba.cache.time.sleep", sleeps.append)

    payload, source = load_or_fetch_league_games_with_source(
        team_id=1610612738,
        team_abbreviation="BOS",
        season="2023-24",
        season_type="Regular Season",
        source_data_dir=tmp_path,
    )

    assert payload["resultSets"][0]["rowSet"] == [["0001"]]
    assert source == "fetched"
    assert len(calls) == 3
    assert sleeps == [0.6, 2.0, 0.6, 4.0, 0.6]

    cached_payload, cached_source = load_or_fetch_league_games_with_source(
        team_id=1610612738,
        team_abbreviation="BOS",
        season="2023-24",
        season_type="Regular Season",
        source_data_dir=tmp_path,
    )

    assert cached_payload == payload
    assert cached_source == "cached"
    assert len(calls) == 3


def test_load_or_fetch_league_games_retries_json_decode_error(
    tmp_path: Path,
    monkeypatch,
):
    calls: list[int] = []
    sleeps: list[float] = []

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            assert kwargs["timeout"] == LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS
            calls.append(1)
            if len(calls) < 3:
                raise json.JSONDecodeError("Expecting value", "", 0)

        def get_dict(self):
            return {"resultSets": [{"headers": ["GAME_ID"], "rowSet": [["0002"]]}]}

    monkeypatch.setattr(
        "wowy.nba.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr("wowy.nba.cache.time.sleep", sleeps.append)

    payload, source = load_or_fetch_league_games_with_source(
        team_id=1610612738,
        team_abbreviation="BOS",
        season="2023-24",
        season_type="Regular Season",
        source_data_dir=tmp_path,
    )

    assert payload["resultSets"][0]["rowSet"] == [["0002"]]
    assert source == "fetched"
    assert len(calls) == 3
    assert sleeps == [0.6, 2.0, 0.6, 4.0, 0.6]


def test_load_or_fetch_box_score_reports_cache_source(tmp_path: Path, monkeypatch):
    calls: list[str] = []

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            calls.append(game_id)

        def get_dict(self):
            return {"resultSets": [{"headers": ["A"], "rowSet": [[1]]}]}

    monkeypatch.setattr(
        "wowy.nba.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr("wowy.nba.cache.time.sleep", lambda _: None)

    payload, source = load_or_fetch_box_score_with_source(
        game_id="0001",
        source_data_dir=tmp_path,
    )
    cached_payload, cached_source = load_or_fetch_box_score_with_source(
        game_id="0001",
        source_data_dir=tmp_path,
    )

    assert payload == cached_payload
    assert source == "fetched"
    assert cached_source == "cached"
    assert calls == ["0001"]


def test_load_or_fetch_box_score_retries_request_exception(tmp_path: Path, monkeypatch):
    calls: list[str] = []
    sleeps: list[float] = []

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            calls.append(game_id)
            if len(calls) < 4:
                raise RequestException("temporary timeout")

        def get_dict(self):
            return {"resultSets": [{"headers": ["A"], "rowSet": [[1]]}]}

    monkeypatch.setattr(
        "wowy.nba.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr("wowy.nba.cache.time.sleep", sleeps.append)

    payload, source = load_or_fetch_box_score_with_source(
        game_id="0002",
        source_data_dir=tmp_path,
    )

    assert payload["resultSets"][0]["rowSet"] == [[1]]
    assert source == "fetched"
    assert calls == ["0002", "0002", "0002", "0002"]
    assert sleeps == [0.6, 2.0, 0.6, 4.0, 0.6, 6.0, 0.6]
