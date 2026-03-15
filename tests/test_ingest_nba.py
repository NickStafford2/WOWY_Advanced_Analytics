from __future__ import annotations

import json
from pathlib import Path

import pytest
from requests import RequestException

from wowy.ingest_nba import (
    build_team_season_artifacts,
    extract_is_home,
    extract_opponent,
    load_player_names_from_cache,
    write_team_season_games_csv,
)
from wowy.io import load_games_from_csv
from wowy.nba_normalize import parse_minutes_to_float, played_in_game
from wowy.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)
from wowy.types import WowyGameRecord


def test_write_team_season_games_csv_writes_normalized_and_derived_outputs(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"

    monkeypatch.setattr(
        "wowy.ingest_nba.teams.find_team_by_abbreviation",
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
        def __init__(self, game_id: str):
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
        "wowy.nba_cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.nba_cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )

    csv_path = tmp_path / "games.csv"
    normalized_games_csv = tmp_path / "normalized" / "games.csv"
    normalized_game_players_csv = tmp_path / "normalized" / "game_players.csv"
    write_team_season_games_csv(
        "BOS",
        "2023-24",
        csv_path,
        normalized_games_csv_path=normalized_games_csv,
        normalized_game_players_csv_path=normalized_game_players_csv,
        source_data_dir=source_data_dir,
    )

    games = load_games_from_csv(csv_path)
    normalized_games = load_normalized_games_from_csv(normalized_games_csv)
    normalized_game_players = load_normalized_game_players_from_csv(
        normalized_game_players_csv
    )

    assert games == [
        WowyGameRecord("0001", "2023-24", "BOS", 12.0, {1628369, 1627759}),
        WowyGameRecord("0002", "2023-24", "BOS", -5.0, {1628369, 1628401}),
    ]
    assert [game.game_date for game in normalized_games] == ["2024-04-01", "2024-04-03"]
    assert [game.opponent for game in normalized_games] == ["LAL", "LAL"]
    assert [game.is_home for game in normalized_games] == [True, False]
    assert [player.player_id for player in normalized_game_players] == [
        1628369,
        1627759,
        999999,
        1628369,
        1628401,
    ]
    assert normalized_game_players[0].minutes == 35.2
    assert normalized_game_players[2].appeared is False

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


def test_write_team_season_games_csv_skips_empty_box_scores(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"

    monkeypatch.setattr(
        "wowy.ingest_nba.teams.find_team_by_abbreviation",
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
        def __init__(self, game_id: str):
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
                        ],
                    },
                    {
                        "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                        "rowSet": [["ATL", -5]],
                    },
                ]
            }

    monkeypatch.setattr(
        "wowy.nba_cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.nba_cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )

    csv_path = tmp_path / "games.csv"
    write_team_season_games_csv(
        "ATL",
        "2023-24",
        csv_path,
        normalized_games_csv_path=tmp_path / "normalized" / "games.csv",
        normalized_game_players_csv_path=tmp_path / "normalized" / "game_players.csv",
        source_data_dir=source_data_dir,
    )

    games = load_games_from_csv(csv_path)

    assert games == [
        WowyGameRecord("0002", "2023-24", "ATL", -5.0, {101, 102}),
    ]


def test_build_team_season_artifacts_returns_normalized_and_derived_outputs(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"

    monkeypatch.setattr(
        "wowy.ingest_nba.teams.find_team_by_abbreviation",
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
        def __init__(self, game_id: str):
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
        "wowy.nba_cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.nba_cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )

    artifacts = build_team_season_artifacts(
        "BOS",
        "2023-24",
        source_data_dir=source_data_dir,
    )

    assert [game.game_id for game in artifacts.normalized_games] == ["0001"]
    assert [player.player_id for player in artifacts.normalized_game_players] == [
        1628369,
        1627759,
    ]
    assert artifacts.wowy_games == [
        WowyGameRecord("0001", "2023-24", "BOS", 12.0, {1628369, 1627759}),
    ]


def test_write_team_season_games_csv_resumes_from_cached_partial_source_data(
    tmp_path: Path,
    monkeypatch,
):
    source_data_dir = tmp_path / "source-data"
    boxscore_calls: list[str] = []

    monkeypatch.setattr(
        "wowy.ingest_nba.teams.find_team_by_abbreviation",
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
        def __init__(self, game_id: str):
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
        "wowy.nba_cache.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.nba_cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )
    monkeypatch.setattr("wowy.nba_cache.time.sleep", lambda _: None)

    csv_path = tmp_path / "games.csv"
    normalized_games_csv = tmp_path / "normalized" / "games.csv"
    normalized_game_players_csv = tmp_path / "normalized" / "game_players.csv"

    with pytest.raises(RequestException):
        write_team_season_games_csv(
            "ATL",
            "2023-24",
            csv_path,
            normalized_games_csv_path=normalized_games_csv,
            normalized_game_players_csv_path=normalized_game_players_csv,
            source_data_dir=source_data_dir,
        )

    assert boxscore_calls == ["0001", "0002", "0002", "0002"]
    assert not csv_path.exists()
    assert not normalized_games_csv.exists()
    assert (source_data_dir / "boxscores/0001_boxscoretraditionalv2.json").exists()

    class RecoveryBoxScoreTraditionalV2:
        def __init__(self, game_id: str):
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
                            ["ATL", 201, "Player 201", "34:00"],
                            ["ATL", 202, "Player 202", "28:15"],
                        ],
                    },
                    {
                        "headers": ["TEAM_ABBREVIATION", "PLUS_MINUS"],
                        "rowSet": [["ATL", -5]],
                    },
                ]
            }

    monkeypatch.setattr(
        "wowy.nba_cache.boxscoretraditionalv2.BoxScoreTraditionalV2",
        RecoveryBoxScoreTraditionalV2,
    )

    write_team_season_games_csv(
        "ATL",
        "2023-24",
        csv_path,
        normalized_games_csv_path=normalized_games_csv,
        normalized_game_players_csv_path=normalized_game_players_csv,
        source_data_dir=source_data_dir,
    )

    games = load_games_from_csv(csv_path)

    assert boxscore_calls == ["0001", "0002", "0002", "0002", "recovery:0002"]
    assert games == [
        WowyGameRecord("0001", "2023-24", "ATL", 7.0, {101, 102}),
        WowyGameRecord("0002", "2023-24", "ATL", -5.0, {201, 202}),
    ]


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
