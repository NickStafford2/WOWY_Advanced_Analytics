from __future__ import annotations

import json
from pathlib import Path

import pytest
from requests import RequestException

from wowy.nba.ingest import (
    build_team_season_artifacts,
    cache_team_season_data,
    extract_is_home,
    extract_opponent,
    load_player_names_from_cache,
)
from wowy.nba.normalize import parse_minutes_to_float, played_in_game
from wowy.data.game_cache_db import (
    load_normalized_game_players_from_db,
    load_normalized_games_from_db,
)
from wowy.apps.wowy.models import WowyGameRecord
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord


def test_cache_team_season_data_writes_normalized_outputs(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"

    monkeypatch.setattr(
        "wowy.nba.ingest.teams.find_team_by_abbreviation",
        lambda abbreviation: {"id": 1610612738, "abbreviation": "BOS"},
    )

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            assert kwargs["team_id_nullable"] == "1610612738"
            assert kwargs["season_nullable"] == "2023-24"
            assert kwargs["season_type_nullable"] == "Regular Season"

        def get_dict(self):
            return {
                "resultSets": [
                    {
                        "headers": ["GAME_ID", "GAME_DATE", "MATCHUP"],
                        "rowSet": [
                            ["0001", "2024-04-01", "BOS vs. LAL"],
                            ["0001", "2024-04-01", "BOS vs. LAL"],
                            ["0002", "2024-04-03", "BOS @ LAL"],
                        ],
                    }
                ]
            }

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int | None = None):
            self.game_id = game_id

        def get_dict(self):
            if self.game_id == "0001":
                return {
                    "resultSets": [
                        {
                            "headers": [
                                "TEAM_ABBREVIATION",
                                "PLAYER_ID",
                                "PLAYER_NAME",
                                "MIN",
                            ],
                            "rowSet": [
                                ["BOS", 1628369, "Jayson Tatum", "35:12"],
                                ["BOS", 1627759, "Jaylen Brown", "34:01"],
                                ["BOS", 1628401, "Derrick White", "55:00"],
                                ["BOS", 1629680, "Al Horford", "55:00"],
                                ["BOS", 1629641, "Kristaps Porzingis", "60:47"],
                                ["BOS", 999999, "Deep Bench", "0:00"],
                                ["LAL", 200000, "Opponent Player", "33:44"],
                            ],
                        },
                        {
                            "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                            "rowSet": [["BOS", 12], ["LAL", -12]],
                        },
                    ]
                }

            return {
                "resultSets": [
                    {
                        "headers": [
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                        ],
                        "rowSet": [
                            ["BOS", 1628369, "Jayson Tatum", "36:00"],
                            ["BOS", 1628401, "Derrick White", "30:15"],
                            ["BOS", 1627759, "Jaylen Brown", "48:00"],
                            ["BOS", 1629680, "Al Horford", "60:00"],
                            ["BOS", 1629641, "Kristaps Porzingis", "65:00"],
                            ["LAL", 200000, "Opponent Player", "31:02"],
                        ],
                    },
                    {
                        "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                        "rowSet": [["BOS", -5], ["LAL", 5]],
                    },
                ]
            }

    monkeypatch.setattr(
        "wowy.nba.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.nba.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )

    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    cache_team_season_data(
        "BOS",
        "2023-24",
        source_data_dir=source_data_dir,
        player_metrics_db_path=db_path,
    )

    normalized_games = load_normalized_games_from_db(
        db_path,
        season_type="Regular Season",
        teams=["BOS"],
        seasons=["2023-24"],
    )
    normalized_game_players = load_normalized_game_players_from_db(
        db_path,
        season_type="Regular Season",
        teams=["BOS"],
        seasons=["2023-24"],
    )

    assert [game.margin for game in normalized_games] == [12.0, -5.0]
    assert [game.game_date for game in normalized_games] == ["2024-04-01", "2024-04-03"]
    assert [game.opponent for game in normalized_games] == ["LAL", "LAL"]
    assert [game.is_home for game in normalized_games] == [True, False]
    assert [player.player_id for player in normalized_game_players] == [
        999999,
        1627759,
        1628369,
        1628401,
        1629641,
        1629680,
        1627759,
        1628369,
        1628401,
        1629641,
        1629680,
    ]
    assert normalized_game_players[2].minutes == 35.2
    assert normalized_game_players[0].appeared is False

    team_season_cache = (
        source_data_dir
        / "team_seasons/BOS_2023-24_regular_season_leaguegamefinder.json"
    )
    box_score_cache = source_data_dir / "boxscores/0001_boxscoretraditionalv2.json"

    assert team_season_cache.exists()
    assert box_score_cache.exists()
    assert json.loads(team_season_cache.read_text(encoding="utf-8"))["resultSets"]

    player_names = load_player_names_from_cache(source_data_dir)
    assert player_names[1628369] == "Jayson Tatum"
    assert player_names[1628401] == "Derrick White"


def test_cache_team_season_data_skips_empty_box_scores(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"

    monkeypatch.setattr(
        "wowy.nba.ingest.teams.find_team_by_abbreviation",
        lambda abbreviation: {"id": 1610612737, "abbreviation": "ATL"},
    )

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            pass

        def get_dict(self):
            return {
                "resultSets": [
                    {
                        "headers": ["GAME_ID", "GAME_DATE", "MATCHUP"],
                        "rowSet": [
                            ["0001", "2024-04-01", "ATL vs. MIL"],
                            ["0002", "2024-04-03", "ATL vs. BOS"],
                        ],
                    }
                ]
            }

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int | None = None):
            self.game_id = game_id

        def get_dict(self):
            if self.game_id == "0001":
                return {
                    "resultSets": [
                        {
                            "headers": [
                                "TEAM_ABBREVIATION",
                                "PLAYER_ID",
                                "PLAYER_NAME",
                                "MIN",
                            ],
                            "rowSet": [],
                        },
                        {
                            "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                            "rowSet": [],
                        },
                    ]
                }

            return {
                "resultSets": [
                    {
                        "headers": [
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                        ],
                        "rowSet": [
                            ["ATL", 101, "Player 101", "36:00"],
                            ["ATL", 102, "Player 102", "30:15"],
                            ["ATL", 103, "Player 103", "48:00"],
                            ["ATL", 104, "Player 104", "60:00"],
                            ["ATL", 105, "Player 105", "65:00"],
                        ],
                    },
                    {
                        "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                        "rowSet": [["ATL", -5]],
                    },
                ]
            }

    monkeypatch.setattr(
        "wowy.nba.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.nba.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )

    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    cache_team_season_data(
        "ATL",
        "2023-24",
        source_data_dir=source_data_dir,
        player_metrics_db_path=db_path,
    )

    games = load_normalized_games_from_db(
        db_path,
        season_type="Regular Season",
        teams=["ATL"],
        seasons=["2023-24"],
    )

    assert [(game.game_id, game.margin) for game in games] == [("0002", -5.0)]


def test_build_team_season_artifacts_returns_normalized_and_derived_outputs(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"

    monkeypatch.setattr(
        "wowy.nba.ingest.teams.find_team_by_abbreviation",
        lambda abbreviation: {"id": 1610612738, "abbreviation": "BOS"},
    )

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            pass

        def get_dict(self):
            return {
                "resultSets": [
                    {
                        "headers": ["GAME_ID", "GAME_DATE", "MATCHUP"],
                        "rowSet": [["0001", "2024-04-01", "BOS vs. LAL"]],
                    }
                ]
            }

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int | None = None):
            self.game_id = game_id

        def get_dict(self):
            return {
                "resultSets": [
                    {
                        "headers": [
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                        ],
                        "rowSet": [
                            ["BOS", 1628369, "Jayson Tatum", "35:12"],
                            ["BOS", 1627759, "Jaylen Brown", "34:01"],
                            ["BOS", 1628401, "Derrick White", "55:00"],
                            ["BOS", 1629680, "Al Horford", "55:00"],
                            ["BOS", 1629641, "Kristaps Porzingis", "60:47"],
                            ["LAL", 200000, "Opponent Player", "33:44"],
                        ],
                    },
                    {
                        "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                        "rowSet": [["BOS", 12], ["LAL", -12]],
                    },
                ]
            }

    monkeypatch.setattr(
        "wowy.nba.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.nba.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )

    result = build_team_season_artifacts(
        "BOS",
        "2023-24",
        source_data_dir=source_data_dir,
    )

    assert [game.game_id for game in result.artifacts.normalized_games] == ["0001"]
    assert [player.player_id for player in result.artifacts.normalized_game_players] == [
        1628369,
        1627759,
        1628401,
        1629680,
        1629641,
    ]
    assert result.artifacts.wowy_games == [
        WowyGameRecord(
            "0001",
            "2023-24",
            "BOS",
            12.0,
            {1628369, 1627759, 1628401, 1629680, 1629641},
        ),
    ]
    assert result.summary.league_games_source == "fetched"
    assert result.summary.fetched_box_scores == 1
    assert result.summary.cached_box_scores == 0
    assert result.summary.processed_games == 1
    assert result.summary.skipped_games == 0


def test_build_team_season_artifacts_supports_historical_team_aliases(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"
    lookup_calls: list[str] = []
    load_calls: list[tuple[int, str, str]] = []
    normalize_calls: list[str] = []

    def fake_find_team_by_abbreviation(abbreviation: str):
        lookup_calls.append(abbreviation)
        if abbreviation == "BKN":
            return {"id": 1610612751, "abbreviation": "BKN"}
        return None

    def fake_load_or_fetch_league_games_with_source(
        *,
        team_id: int,
        team_abbreviation: str,
        season: str,
        season_type: str,
        source_data_dir: Path,
        log,
    ):
        load_calls.append((team_id, team_abbreviation, season))
        return (
            {
                "resultSets": [
                    {
                        "headers": ["GAME_ID", "GAME_DATE", "MATCHUP"],
                        "rowSet": [["0001", "2010-04-01", "NJN vs. BOS"]],
                    }
                ]
            },
            "fetched",
        )

    def fake_fetch_normalized_game_data_with_source(
        *,
        game_id: str,
        team_abbreviation: str,
        season: str,
        game_date: str,
        opponent: str,
        is_home: bool,
        season_type: str,
        source_data_dir: Path,
        log,
    ):
        normalize_calls.append(team_abbreviation)
        return (
            NormalizedGameRecord(
                game_id=game_id,
                season=season,
                game_date=game_date,
                team=team_abbreviation,
                opponent=opponent,
                is_home=is_home,
                margin=5.0,
                season_type=season_type,
                source="nba_api",
            ),
            [
                NormalizedGamePlayerRecord(
                    game_id=game_id,
                    team=team_abbreviation,
                    player_id=101,
                    player_name="Player 101",
                    appeared=True,
                    minutes=36.0,
                )
            ],
            "fetched",
        )

    monkeypatch.setattr(
        "wowy.nba.ingest.teams.find_team_by_abbreviation",
        fake_find_team_by_abbreviation,
    )
    monkeypatch.setattr(
        "wowy.nba.ingest.load_or_fetch_league_games_with_source",
        fake_load_or_fetch_league_games_with_source,
    )
    monkeypatch.setattr(
        "wowy.nba.ingest.fetch_normalized_game_data_with_source",
        fake_fetch_normalized_game_data_with_source,
    )

    result = build_team_season_artifacts(
        "NJN",
        "2009-10",
        source_data_dir=source_data_dir,
    )

    assert lookup_calls == ["BKN"]
    assert load_calls == [(1610612751, "NJN", "2009-10")]
    assert normalize_calls == ["NJN"]
    assert result.summary.team == "NJN"
    assert result.artifacts.normalized_games[0].team == "NJN"


def test_cache_team_season_data_resumes_from_cached_partial_source_data(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"
    boxscore_calls: list[str] = []

    monkeypatch.setattr(
        "wowy.nba.ingest.teams.find_team_by_abbreviation",
        lambda abbreviation: {"id": 1610612737, "abbreviation": "ATL"},
    )

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            pass

        def get_dict(self):
            return {
                "resultSets": [
                    {
                        "headers": ["GAME_ID", "GAME_DATE", "MATCHUP"],
                        "rowSet": [
                            ["0001", "2024-04-01", "ATL vs. MIL"],
                            ["0002", "2024-04-03", "ATL vs. BOS"],
                        ],
                    }
                ]
            }

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int | None = None):
            self.game_id = game_id
            boxscore_calls.append(game_id)

        def get_dict(self):
            if self.game_id == "0001":
                return {
                    "resultSets": [
                        {
                            "headers": [
                                "TEAM_ABBREVIATION",
                                "PLAYER_ID",
                                "PLAYER_NAME",
                                "MIN",
                            ],
                            "rowSet": [
                                ["ATL", 101, "Player 101", "36:00"],
                                ["ATL", 102, "Player 102", "30:15"],
                                ["ATL", 103, "Player 103", "48:00"],
                                ["ATL", 104, "Player 104", "60:00"],
                                ["ATL", 105, "Player 105", "65:00"],
                            ],
                        },
                        {
                            "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                            "rowSet": [["ATL", 7]],
                        },
                    ]
                }
            raise RequestException("temporary failure")

    monkeypatch.setattr(
        "wowy.nba.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.nba.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr("wowy.nba.cache.time.sleep", lambda _: None)

    db_path = tmp_path / "app" / "player_metrics.sqlite3"

    with pytest.raises(RequestException):
        cache_team_season_data(
            "ATL",
            "2023-24",
            source_data_dir=source_data_dir,
            player_metrics_db_path=db_path,
        )

    assert boxscore_calls == ["0001", "0002", "0002", "0002", "0002", "0002"]
    assert (source_data_dir / "boxscores/0001_boxscoretraditionalv2.json").exists()

    class RecoveryBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int | None = None):
            self.game_id = game_id
            boxscore_calls.append(f"recovery:{game_id}")

        def get_dict(self):
            return {
                "resultSets": [
                    {
                        "headers": [
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                        ],
                        "rowSet": [
                            ["ATL", 201, "Player 201", "36:00"],
                            ["ATL", 202, "Player 202", "30:15"],
                            ["ATL", 203, "Player 203", "48:00"],
                            ["ATL", 204, "Player 204", "60:00"],
                            ["ATL", 205, "Player 205", "65:00"],
                        ],
                    },
                    {
                        "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                        "rowSet": [["ATL", -5]],
                    },
                ]
            }

    monkeypatch.setattr(
        "wowy.nba.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        RecoveryBoxScoreTraditionalV2,
    )

    summary = cache_team_season_data(
        "ATL",
        "2023-24",
        source_data_dir=source_data_dir,
        player_metrics_db_path=db_path,
    )

    games = load_normalized_games_from_db(
        db_path,
        season_type="Regular Season",
        teams=["ATL"],
        seasons=["2023-24"],
    )

    assert boxscore_calls == [
        "0001",
        "0002",
        "0002",
        "0002",
        "0002",
        "0002",
        "recovery:0002",
    ]
    assert summary.league_games_source == "cached"
    assert summary.fetched_box_scores == 1
    assert summary.cached_box_scores == 1
    assert summary.processed_games == 2
    assert summary.skipped_games == 0
    assert [(game.game_id, game.margin) for game in games] == [("0001", 7.0), ("0002", -5.0)]


def test_cache_team_season_data_raises_on_inconsistent_outputs(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"

    monkeypatch.setattr(
        "wowy.nba.ingest.teams.find_team_by_abbreviation",
        lambda abbreviation: {"id": 1610612738, "abbreviation": "BOS"},
    )

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            pass

        def get_dict(self):
            return {
                "resultSets": [
                    {
                        "headers": ["GAME_ID", "GAME_DATE", "MATCHUP"],
                        "rowSet": [["0001", "2024-04-01", "BOS vs. LAL"]],
                    }
                ]
            }

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str, timeout: int | None = None):
            self.game_id = game_id

        def get_dict(self):
            return {
                "resultSets": [
                    {
                        "headers": [
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                        ],
                        "rowSet": [
                            ["BOS", 1628369, "Jayson Tatum", "35:12"],
                            ["BOS", 1627759, "Jaylen Brown", "34:01"],
                            ["BOS", 1628401, "Derrick White", "55:00"],
                            ["BOS", 1629680, "Al Horford", "55:00"],
                            ["BOS", 1629641, "Kristaps Porzingis", "60:47"],
                            ["LAL", 200000, "Opponent Player", "33:44"],
                        ],
                    },
                    {
                        "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                        "rowSet": [["BOS", 12], ["LAL", -12]],
                    },
                ]
            }

    monkeypatch.setattr(
        "wowy.nba.cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.nba.cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr(
        "wowy.nba.ingest.validate_team_season_records",
        lambda *args: "wowy_data",
    )

    with pytest.raises(ValueError, match="Inconsistent team-season cache"):
        cache_team_season_data(
            "BOS",
            "2023-24",
            source_data_dir=source_data_dir,
            player_metrics_db_path=tmp_path / "app" / "player_metrics.sqlite3",
        )


def test_extract_matchup_fields_accept_requested_team_on_either_side() -> None:
    home_row = {"GAME_ID": "0001", "MATCHUP": "MIA @ WAS"}
    away_row = {"GAME_ID": "0002", "MATCHUP": "WAS @ MIA"}

    assert extract_opponent(home_row, "WAS") == "MIA"
    assert extract_is_home(home_row, "WAS") is True
    assert extract_opponent(away_row, "WAS") == "MIA"
    assert extract_is_home(away_row, "WAS") is False


@pytest.mark.parametrize(
    ("minutes", "expected"),
    [
        ("35:12", True),
        ("12", True),
        ("0", False),
        ("0:00", False),
        ("00:00", False),
        ("0.0", False),
        ("", False),
        (None, False),
        ("DNP", False),
        ("DND", False),
        ("NWT", False),
    ],
)
def test_played_in_game_handles_numeric_and_status_values(
    minutes: object,
    expected: bool,
):
    assert played_in_game(minutes) is expected


def test_parse_minutes_to_float_returns_none_for_status_text():
    assert parse_minutes_to_float("DNP") is None
