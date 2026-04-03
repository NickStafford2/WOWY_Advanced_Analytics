from __future__ import annotations

import json
from pathlib import Path

import pytest
from requests import RequestException

from rawr_analytics.data.game_cache.repository import (
    load_cache_load_row,
    load_normalized_games_from_db,
    load_normalized_scope_records_from_db,
    replace_team_season_normalized_rows,
)
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.services._ingest_errors import BoxScoreFetchError, LeagueGamesFetchError
from rawr_analytics.basketball.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.sources.nba_api.cache import (
    BOX_SCORE_REQUEST_TIMEOUT_SECONDS,
    LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS,
    _league_games_cache_path,
    load_cached_payload,
    load_or_fetch_box_score_cache,
    load_or_fetch_league_games,
    _write_cached_payload,
)
from rawr_analytics.shared.scope import TeamSeasonScope
from tests.support import game as normalized_game
from tests.support import player as normalized_player
from tests.support import seed_db_from_team_seasons


def test_write_cached_payload_writes_json_atomically(tmp_path: Path):
    cache_path = tmp_path / "cache" / "payload.json"

    _write_cached_payload(cache_path, {"value": 1})

    assert cache_path.exists()
    assert json.loads(cache_path.read_text(encoding="utf-8")) == {"value": 1}
    assert not list(cache_path.parent.glob("*.tmp-*"))


def test_load_cached_payload_ignores_corrupt_json(tmp_path: Path):
    cache_path = tmp_path / "cache" / "payload.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text('{"value": ', encoding="utf-8")

    assert load_cached_payload(cache_path) is None


def test_load_cached_payload_discards_corrupt_json_file(tmp_path: Path):
    cache_path = tmp_path / "cache" / "payload.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text('{"value": ', encoding="utf-8")

    assert load_cached_payload(cache_path) is None
    assert not cache_path.exists()


def test_load_cached_payload_discards_non_object_json_file(tmp_path: Path):
    cache_path = tmp_path / "cache" / "payload.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text('["not", "an", "object"]', encoding="utf-8")

    assert load_cached_payload(cache_path) is None
    assert not cache_path.exists()


def test_load_normalized_scope_records_from_db_requires_each_cached_team_season(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(
        db_path,
        [
            (
                "BOS",
                "2023-24",
                [
                    normalized_game(
                        "1",
                        "2023-24",
                        "2024-04-01",
                        "BOS",
                        "MIL",
                        True,
                        10.0,
                    )
                ],
                [
                    normalized_player("1", "BOS", 101, "Player 101", True, 34.0),
                    normalized_player("1", "BOS", 102, "Player 102", True, 32.0),
                    normalized_player("1", "BOS", 103, "Player 103", True, 30.0),
                    normalized_player("1", "BOS", 104, "Player 104", True, 28.0),
                    normalized_player("1", "BOS", 105, "Player 105", True, 26.0),
                ],
            )
        ],
    )

    with pytest.raises(
        ValueError,
        match="Missing cached team-season scope for MIL 2023-24 Regular Season",
    ):
        load_normalized_scope_records_from_db(
            db_path,
            team_seasons=[
                TeamSeasonScope("BOS", "2023-24", todo),
                TeamSeasonScope("MIL", "2023-24", todo),
            ],
            season_type="Regular Season",
        )


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
        "rawr_analytics.sources.nba_api.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", sleeps.append)

    payload, source = load_or_fetch_league_games(
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

    cached_payload, cached_source = load_or_fetch_league_games(
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
        "rawr_analytics.sources.nba_api.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", sleeps.append)

    payload, source = load_or_fetch_league_games(
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


def test_load_or_fetch_league_games_raises_typed_fetch_error_after_retries(
    tmp_path: Path,
    monkeypatch,
):
    calls: list[int] = []
    sleeps: list[float] = []

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            assert kwargs["timeout"] == LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS
            calls.append(1)
            raise json.JSONDecodeError("Expecting value", "", 0)

    monkeypatch.setattr(
        "rawr_analytics.sources.nba_api.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", sleeps.append)

    with pytest.raises(LeagueGamesFetchError, match="Failed to fetch league games"):
        load_or_fetch_league_games(
            team_id=1610612738,
            team_abbreviation="BOS",
            season="2023-24",
            season_type="Regular Season",
            source_data_dir=tmp_path,
        )

    assert len(calls) == 3
    assert sleeps == [0.6, 2.0, 0.6, 4.0, 0.6]


def test_load_or_fetch_league_games_discards_empty_cached_payload_and_refetches(
    tmp_path: Path,
    monkeypatch,
):
    cache_path = _league_games_cache_path(
        team_abbreviation="BOS",
        season="2023-24",
        season_type="Regular Season",
        source_data_dir=tmp_path,
    )
    _write_cached_payload(cache_path, {"resultSets": [{"headers": ["GAME_ID"], "rowSet": []}]})

    calls: list[int] = []

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            assert kwargs["timeout"] == LEAGUE_GAMES_REQUEST_TIMEOUT_SECONDS
            calls.append(1)

        def get_dict(self):
            return {"resultSets": [{"headers": ["GAME_ID"], "rowSet": [["0003"]]}]}

    monkeypatch.setattr(
        "rawr_analytics.sources.nba_api.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", lambda _: None)

    payload, source = load_or_fetch_league_games(
        team_id=1610612738,
        team_abbreviation="BOS",
        season="2023-24",
        season_type="Regular Season",
        source_data_dir=tmp_path,
        log=None,
    )

    assert source == "fetched"
    assert payload["resultSets"][0]["rowSet"] == [["0003"]]
    assert calls == [1]
    assert json.loads(cache_path.read_text(encoding="utf-8")) == payload


def test_load_or_fetch_box_score_reports_cache_source(tmp_path: Path, monkeypatch):
    calls: list[str] = []

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            calls.append(game_id)

        def get_dict(self):
            return {"resultSets": [{"headers": ["A"], "rowSet": [[1]]}]}

    monkeypatch.setattr(
        "rawr_analytics.sources.nba_api.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", lambda _: None)

    payload, source = load_or_fetch_box_score_cache(
        game_id="0001",
        source_data_dir=tmp_path,
    )
    cached_payload, cached_source = load_or_fetch_box_score_cache(
        game_id="0001",
        source_data_dir=tmp_path,
    )

    assert payload == cached_payload
    assert source == "fetched"
    assert cached_source == "cached"
    assert calls == ["0001"]


def test_load_or_fetch_box_score_falls_back_to_v3_when_v2_is_empty(tmp_path: Path, monkeypatch):
    v2_calls: list[str] = []
    v3_calls: list[str] = []

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            v2_calls.append(game_id)

        def get_dict(self):
            return {
                "resultSets": [
                    {"name": "PlayerStats", "headers": ["A"], "rowSet": []},
                    {"name": "TeamStats", "headers": ["B"], "rowSet": []},
                ]
            }

    class FakeBoxScoreTraditionalV3:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            v3_calls.append(game_id)

        def get_dict(self):
            return {
                "resultSets": {
                    "PlayerStats": {
                        "headers": ["personId"],
                        "data": [[1]],
                    },
                    "TeamStats": {
                        "headers": ["teamId"],
                        "data": [[2]],
                    },
                }
            }

    monkeypatch.setattr(
        "rawr_analytics.sources.nba_api.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr(
        "rawr_analytics.sources.nba_api.cache.boxscoretraditionalv3.BoxScoreTraditionalV3",
        FakeBoxScoreTraditionalV3,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", lambda _: None)

    payload, source = load_or_fetch_box_score_cache(
        game_id="0003",
        source_data_dir=tmp_path,
    )
    cached_payload, cached_source = load_or_fetch_box_score_cache(
        game_id="0003",
        source_data_dir=tmp_path,
    )

    assert source == "fetched"
    assert cached_source == "cached"
    assert payload == cached_payload
    assert v2_calls == ["0003"]
    assert v3_calls == ["0003"]
    assert (tmp_path / "boxscores" / "0003_boxscoretraditionalv3.json").exists()


def test_load_or_fetch_box_score_falls_back_to_live_when_v2_and_v3_are_empty(
    tmp_path: Path,
    monkeypatch,
):
    v2_calls: list[str] = []
    v3_calls: list[str] = []
    live_calls: list[str] = []

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            v2_calls.append(game_id)

        def get_dict(self):
            return {
                "resultSets": [
                    {"name": "PlayerStats", "headers": ["A"], "rowSet": []},
                    {"name": "TeamStats", "headers": ["B"], "rowSet": []},
                ]
            }

    class FakeBoxScoreTraditionalV3:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            v3_calls.append(game_id)

        def get_dict(self):
            return {
                "resultSets": {
                    "PlayerStats": {"headers": [], "data": []},
                    "TeamStats": {"headers": [], "data": []},
                }
            }

    class FakeLiveBoxScore:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            live_calls.append(game_id)

        def get_dict(self):
            return {
                "game": {
                    "homeTeam": {"players": [{"personId": 1}], "statistics": {}},
                    "awayTeam": {"players": [{"personId": 2}], "statistics": {}},
                }
            }

    monkeypatch.setattr(
        "rawr_analytics.sources.nba_api.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr(
        "rawr_analytics.sources.nba_api.cache.boxscoretraditionalv3.BoxScoreTraditionalV3",
        FakeBoxScoreTraditionalV3,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.live_boxscore.BoxScore", FakeLiveBoxScore)
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", lambda _: None)

    payload, source = load_or_fetch_box_score_cache(
        game_id="0004",
        source_data_dir=tmp_path,
    )
    cached_payload, cached_source = load_or_fetch_box_score_cache(
        game_id="0004",
        source_data_dir=tmp_path,
    )

    assert source == "fetched"
    assert cached_source == "cached"
    assert payload == cached_payload
    assert v2_calls == ["0004"]
    assert v3_calls == ["0004"]
    assert live_calls == ["0004"]
    assert (tmp_path / "boxscores" / "0004_boxscorelive.json").exists()


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
        "rawr_analytics.sources.nba_api.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", sleeps.append)

    payload, source = load_or_fetch_box_score_cache(
        game_id="0002",
        source_data_dir=tmp_path,
    )

    assert payload["resultSets"][0]["rowSet"] == [[1]]
    assert source == "fetched"
    assert calls == ["0002", "0002", "0002", "0002"]
    assert sleeps == [0.6, 2.0, 0.6, 4.0, 0.6, 6.0, 0.6]


def test_load_or_fetch_box_score_raises_typed_fetch_error_after_retries(
    tmp_path: Path,
    monkeypatch,
):
    calls: list[str] = []
    sleeps: list[float] = []

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            calls.append(game_id)
            raise RequestException("temporary timeout")

        def get_dict(self):
            raise AssertionError("unreachable")

    monkeypatch.setattr(
        "rawr_analytics.sources.nba_api.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", sleeps.append)

    with pytest.raises(BoxScoreFetchError, match="Failed to fetch box score"):
        load_or_fetch_box_score_cache(
            game_id="0002",
            source_data_dir=tmp_path,
        )

    assert calls == ["0002", "0002", "0002", "0002", "0002"]
    assert sleeps == [0.6, 2.0, 0.6, 4.0, 0.6, 6.0, 0.6, 8.0, 0.6]


def test_load_or_fetch_box_score_discards_empty_cached_payload_and_refetches(
    tmp_path: Path,
    monkeypatch,
):
    cache_path = tmp_path / "boxscores" / "0009_boxscoretraditionalv2.json"
    _write_cached_payload(
        cache_path,
        {
            "resultSets": [
                {"name": "PlayerStats", "headers": ["A"], "rowSet": []},
                {"name": "TeamStats", "headers": ["B"], "rowSet": []},
            ]
        },
    )

    calls: list[str] = []

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int):
            assert timeout == BOX_SCORE_REQUEST_TIMEOUT_SECONDS
            calls.append(game_id)

        def get_dict(self):
            return {"resultSets": [{"headers": ["A"], "rowSet": [[1]]}]}

    monkeypatch.setattr(
        "rawr_analytics.sources.nba_api.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr("rawr_analytics.sources.nba_api.cache.time.sleep", lambda _: None)

    payload, source = load_or_fetch_box_score_cache(
        game_id="0009",
        source_data_dir=tmp_path,
        log_fn=None,
    )

    assert source == "fetched"
    assert payload["resultSets"][0]["rowSet"] == [[1]]
    assert calls == ["0009"]
    assert json.loads(cache_path.read_text(encoding="utf-8")) == payload


def test_canonicalize_season_type_accepts_common_aliases():
    assert canonicalize_season_type("regular season") == "Regular Season"
    assert canonicalize_season_type("postseason") == "Playoffs"
    assert canonicalize_season_type("Playoff") == "Playoffs"


def test_league_games_cache_path_uses_canonical_season_type_slug(tmp_path: Path):
    cache_path = _league_games_cache_path(
        team_abbreviation="BOS",
        season="2023-24",
        season_type="postseason",
        source_data_dir=tmp_path,
    )

    assert cache_path == (tmp_path / "team_seasons" / "BOS_2023-24_playoffs_leaguegamefinder.json")


def test_team_id_authoritative_reads_match_historical_alias_scopes():
    replace_team_season_normalized_rows(
        team_id=todo,
        season="2009-10",
        season_type="Regular Season",
        games=[normalized_game("0001", "2009-10", "2010-04-01", "NJN", "BOS", True, 3.0)],
        game_players=[
            normalized_player("0001", "NJN", 101, "Player 101", True, 48.0),
            normalized_player("0001", "NJN", 102, "Player 102", True, 48.0),
            normalized_player("0001", "NJN", 103, "Player 103", True, 48.0),
            normalized_player("0001", "NJN", 104, "Player 104", True, 48.0),
            normalized_player("0001", "NJN", 105, "Player 105", True, 48.0),
        ],
        source_path="sqlite://normalized_games/NJN_2009-10_regular_season",
        source_snapshot="test",
        source_kind="unit-test",
    )

    games = load_normalized_games_from_db(
        season_type="Regular Season",
        team_ids=["todo/fix"],
        seasons=["2009-10"],
    )
    load_row = load_cache_load_row(
        team_id="todo",
        season="2009-10",
        season_type="Regular Season",
    )

    assert [(game.game_id, game.team, game.team_id) for game in games] == [
        ("0001", "NJN", 1610612751)
    ]
    assert load_row is not None
    assert load_row.team == "NJN"
    assert load_row.team_id == 1610612751


def test_resolve_team_seasons_keeps_original_hornets_historical_scope(tmp_path: Path):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    replace_team_season_normalized_rows(
        db_path,
        team_id=todo,
        season="2001-02",
        season_type="Regular Season",
        games=[normalized_game("0001", "2001-02", "2002-04-01", "CHH", "DET", True, 3.0)],
        game_players=[
            NormalizedGamePlayerRecord("0001", 101, "Player 101", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", 102, "Player 102", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", 103, "Player 103", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", 104, "Player 104", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", 105, "Player 105", True, 48.0, 1610612766),
        ],
        source_path="sqlite://normalized_games/CHH_2001-02_regular_season",
        source_snapshot="test",
        source_kind="unit-test",
    )

    resolved = resolve_team_seasons(
        team_ids=["todo This may not be needed now that team_ids are used everywhere."],
        seasons=["2001-02"],
        season_type="Regular Season",
    )

    assert resolved == [TeamSeasonScope(season="2001-02", team_id=1610612766)]


def test_resolve_team_seasons_accepts_team_id_for_historical_multi_season_scope():
    replace_team_season_normalized_rows(
        team_id=resolve_team_id("NOH", season="2002-03"),
        season="2002-03",
        season_type="Regular Season",
        games=[normalized_game("0001", "2002-03", "2003-04-01", "NOH", "BOS", True, 3.0)],
        game_players=[
            normalized_player("0001", "NOH", 101, "Player 101", True, 48.0),
            normalized_player("0001", "NOH", 102, "Player 102", True, 48.0),
            normalized_player("0001", "NOH", 103, "Player 103", True, 48.0),
            normalized_player("0001", "NOH", 104, "Player 104", True, 48.0),
            normalized_player("0001", "NOH", 105, "Player 105", True, 48.0),
        ],
        source_path="sqlite://normalized_games/NOH_2002-03_regular_season",
        source_snapshot="test",
        source_kind="unit-test",
    )
    replace_team_season_normalized_rows(
        team_id=resolve_team_id("NOP", season="2013-14"),
        season="2013-14",
        season_type="Regular Season",
        games=[normalized_game("0002", "2013-14", "2014-04-01", "NOP", "LAL", True, 2.0)],
        game_players=[
            normalized_player("0002", "NOP", 102, "Player 102", True, 48.0),
            normalized_player("0002", "NOP", 103, "Player 103", True, 48.0),
            normalized_player("0002", "NOP", 104, "Player 104", True, 48.0),
            normalized_player("0002", "NOP", 105, "Player 105", True, 48.0),
            normalized_player("0002", "NOP", 106, "Player 106", True, 48.0),
        ],
        source_path="sqlite://normalized_games/NOP_2013-14_regular_season",
        source_snapshot="test",
        source_kind="unit-test",
    )

    resolved = resolve_team_seasons(
        seasons=["2002-03", "2013-14"],
        team_ids=[1610612740],
        season_type="Regular Season",
    )

    assert resolved == [
        TeamSeasonScope(season="2002-03", team_id=1610612740),
        TeamSeasonScope(season="2013-14", team_id=1610612740),
    ]


def test_load_cache_load_row_uses_season_scoped_original_hornets_identity(tmp_path: Path):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    replace_team_season_normalized_rows(
        db_path,
        team="CHH",
        team_id=resolve_team_id("CHH", season="2001-02"),
        season="2001-02",
        season_type="Regular Season",
        games=[normalized_game("0001", "2001-02", "2002-04-01", "CHH", "DET", True, 3.0)],
        game_players=[
            NormalizedGamePlayerRecord("0001", "CHH", 101, "Player 101", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", "CHH", 102, "Player 102", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", "CHH", 103, "Player 103", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", "CHH", 104, "Player 104", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", "CHH", 105, "Player 105", True, 48.0, 1610612766),
        ],
        source_path="sqlite://normalized_games/CHH_2001-02_regular_season",
        source_snapshot="test",
        source_kind="unit-test",
    )

    games = load_normalized_games_from_db(
        db_path,
        season_type="Regular Season",
        teams=["CHH"],
        seasons=["2001-02"],
    )
    load_row = load_cache_load_row(
        db_path,
        team="CHH",
        season="2001-02",
        season_type="Regular Season",
    )

    assert [(game.game_id, game.team, game.team_id) for game in games] == [
        ("0001", "CHH", 1610612766)
    ]
    assert load_row is not None
    assert load_row.team == "CHH"
    assert load_row.team_id == 1610612766


def test_load_normalized_games_handles_historical_team_filters_across_multiple_seasons(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    replace_team_season_normalized_rows(
        db_path,
        team="CHH",
        team_id=resolve_team_id("CHH", season="2001-02"),
        season="2001-02",
        season_type="Regular Season",
        games=[normalized_game("0001", "2001-02", "2002-04-01", "CHH", "DET", True, 3.0)],
        game_players=[
            NormalizedGamePlayerRecord("0001", "CHH", 101, "Player 101", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", "CHH", 102, "Player 102", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", "CHH", 103, "Player 103", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", "CHH", 104, "Player 104", True, 48.0, 1610612766),
            NormalizedGamePlayerRecord("0001", "CHH", 105, "Player 105", True, 48.0, 1610612766),
        ],
        source_path="sqlite://normalized_games/CHH_2001-02_regular_season",
        source_snapshot="test",
        source_kind="unit-test",
    )
    replace_team_season_normalized_rows(
        db_path,
        team="NOH",
        team_id=resolve_team_id("NOH", season="2002-03"),
        season="2002-03",
        season_type="Regular Season",
        games=[normalized_game("0002", "2002-03", "2003-03-10", "NOH", "BOS", True, 8.0)],
        game_players=[
            normalized_player("0002", "NOH", 201, "Player 201", True, 48.0),
            normalized_player("0002", "NOH", 202, "Player 202", True, 48.0),
            normalized_player("0002", "NOH", 203, "Player 203", True, 48.0),
            normalized_player("0002", "NOH", 204, "Player 204", True, 48.0),
            normalized_player("0002", "NOH", 205, "Player 205", True, 48.0),
        ],
        source_path="sqlite://normalized_games/NOH_2002-03_regular_season",
        source_snapshot="test",
        source_kind="unit-test",
    )

    games = load_normalized_games_from_db(
        db_path,
        season_type="Regular Season",
        teams=["CHH", "NOH"],
        seasons=["2001-02", "2002-03"],
    )

    assert [(game.game_id, game.season, game.team, game.team_id) for game in games] == [
        ("0001", "2001-02", "CHH", 1610612766),
        ("0002", "2002-03", "NOH", 1610612740),
    ]


def test_replace_team_season_normalized_rows_rejects_non_canonical_or_implausible_data(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    with pytest.raises(ValueError, match="expected '2023-24'"):
        replace_team_season_normalized_rows(
            db_path,
            team="BOS",
            team_id=resolve_team_id("BOS", season="2023-24"),
            season="2023-24",
            season_type="Regular Season",
            games=[
                NormalizedGameRecord(
                    game_id="0001",
                    season="2022-23",
                    game_date="2024-04-01",
                    team="BOS",
                    opponent="LAL",
                    team_id=1610612738,
                    opponent_team_id=1610612747,
                    is_home=True,
                    margin=8.0,
                    season_type="Regular Season",
                    source="nba_api",
                )
            ],
            game_players=[
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 101, "Player 101", True, 48.0, 1610612738
                ),
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 102, "Player 102", True, 48.0, 1610612738
                ),
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 103, "Player 103", True, 48.0, 1610612738
                ),
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 104, "Player 104", True, 48.0, 1610612738
                ),
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 105, "Player 105", True, 48.0, 1610612738
                ),
            ],
            source_path="sqlite://normalized_games/BOS_2023-24_regular_season",
            source_snapshot="test",
            source_kind="unit-test",
        )


def test_replace_team_season_normalized_rows_rejects_historically_wrong_team_label(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    with pytest.raises(ValueError, match="Canonical game '0001' has team 'NOP'; expected 'NOH'"):
        replace_team_season_normalized_rows(
            db_path,
            team="NOP",
            team_id=1610612740,
            season="2002-03",
            season_type="Regular Season",
            games=[
                NormalizedGameRecord(
                    game_id="0001",
                    season="2002-03",
                    game_date="2003-03-10",
                    team="NOP",
                    opponent="BOS",
                    team_id=1610612740,
                    opponent_team_id=1610612738,
                    is_home=True,
                    margin=8.0,
                    season_type="Regular Season",
                    source="nba_api",
                )
            ],
            game_players=[
                NormalizedGamePlayerRecord(
                    "0001", "NOP", 101, "Player 101", True, 48.0, 1610612740
                ),
                NormalizedGamePlayerRecord(
                    "0001", "NOP", 102, "Player 102", True, 48.0, 1610612740
                ),
                NormalizedGamePlayerRecord(
                    "0001", "NOP", 103, "Player 103", True, 48.0, 1610612740
                ),
                NormalizedGamePlayerRecord(
                    "0001", "NOP", 104, "Player 104", True, 48.0, 1610612740
                ),
                NormalizedGamePlayerRecord(
                    "0001", "NOP", 105, "Player 105", True, 48.0, 1610612740
                ),
            ],
            source_path="sqlite://normalized_games/NOP_2002-03_regular_season",
            source_snapshot="test",
            source_kind="unit-test",
        )


def test_replace_team_season_normalized_rows_rejects_opponent_label_mismatch(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    with pytest.raises(
        ValueError, match="opponent 'LAL' does not match opponent_team_id 1610612744"
    ):
        replace_team_season_normalized_rows(
            db_path,
            team="BOS",
            team_id=resolve_team_id("BOS", season="2023-24"),
            season="2023-24",
            season_type="Regular Season",
            games=[
                NormalizedGameRecord(
                    game_id="0001",
                    season="2023-24",
                    game_date="2024-04-01",
                    team="BOS",
                    opponent="LAL",
                    team_id=1610612738,
                    opponent_team_id=1610612744,
                    is_home=True,
                    margin=8.0,
                    season_type="Regular Season",
                    source="nba_api",
                )
            ],
            game_players=[
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 101, "Player 101", True, 48.0, 1610612738
                ),
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 102, "Player 102", True, 48.0, 1610612738
                ),
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 103, "Player 103", True, 48.0, 1610612738
                ),
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 104, "Player 104", True, 48.0, 1610612738
                ),
                NormalizedGamePlayerRecord(
                    "0001", "BOS", 105, "Player 105", True, 48.0, 1610612738
                ),
            ],
            source_path="sqlite://normalized_games/BOS_2023-24_regular_season",
            source_snapshot="test",
            source_kind="unit-test",
        )

        with pytest.raises(ValueError, match="positive minutes"):
            replace_team_season_normalized_rows(
                db_path,
                team="BOS",
                team_id=resolve_team_id("BOS", season="2023-24"),
                season="2023-24",
                season_type="Regular Season",
                games=[
                    NormalizedGameRecord(
                        game_id="0002",
                        season="2023-24",
                        game_date="2024-04-03",
                        team="BOS",
                        opponent="LAL",
                        team_id=1610612738,
                        opponent_team_id=1610612747,
                        is_home=False,
                        margin=-4.0,
                        season_type="Regular Season",
                        source="nba_api",
                    )
                ],
                game_players=[
                    NormalizedGamePlayerRecord(
                        "0002", "BOS", 101, "Player 101", True, None, 1610612738
                    ),
                    NormalizedGamePlayerRecord(
                        "0002", "BOS", 102, "Player 102", True, 60.0, 1610612738
                    ),
                    NormalizedGamePlayerRecord(
                        "0002", "BOS", 103, "Player 103", True, 60.0, 1610612738
                    ),
                    NormalizedGamePlayerRecord(
                        "0002", "BOS", 104, "Player 104", True, 60.0, 1610612738
                    ),
                    NormalizedGamePlayerRecord(
                        "0002", "BOS", 105, "Player 105", True, 60.0, 1610612738
                    ),
                ],
                source_path="sqlite://normalized_games/BOS_2023-24_regular_season",
                source_snapshot="test",
                source_kind="unit-test",
            )

    with pytest.raises(ValueError, match="Implausible total appeared minutes"):
        replace_team_season_normalized_rows(
            db_path,
            team_id=resolve_team_id("BOS", season="2023-24"),
            season="2023-24",
            season_type="Regular Season",
            games=[
                NormalizedGameRecord(
                    game_id="0003",
                    season="2023-24",
                    game_date="2024-04-05",
                    team_id=1610612738,
                    opponent_team_id=1610612747,
                    is_home=True,
                    margin=2.0,
                    season_type="Regular Season",
                    source="nba_api",
                )
            ],
            game_players=[
                NormalizedGamePlayerRecord("0003", 101, "Player 101", True, 20.0, 1610612738),
                NormalizedGamePlayerRecord("0003", 102, "Player 102", True, 20.0, 1610612738),
                NormalizedGamePlayerRecord("0003", 103, "Player 103", True, 20.0, 1610612738),
                NormalizedGamePlayerRecord("0003", 104, "Player 104", True, 20.0, 1610612738),
                NormalizedGamePlayerRecord("0003", 105, "Player 105", True, 20.0, 1610612738),
            ],
            source_path="sqlite://normalized_games/BOS_2023-24_regular_season",
            source_snapshot="test",
            source_kind="unit-test",
        )
