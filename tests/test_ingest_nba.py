from __future__ import annotations

import json
from pathlib import Path

from wowy.ingest_nba import load_player_names_from_cache, write_team_season_games_csv
from wowy.io import load_games_from_csv


def test_write_team_season_games_csv_uses_existing_games_shape(
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
                        "headers": ["GAME_ID"],
                        "rowSet": [["0001"], ["0001"], ["0002"]],
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
        "wowy.ingest_nba.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.ingest_nba.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )

    csv_path = tmp_path / "games.csv"
    write_team_season_games_csv(
        "BOS",
        "2023-24",
        csv_path,
        source_data_dir=source_data_dir,
    )

    games = load_games_from_csv(csv_path)

    assert games == [
        {
            "game_id": "0001",
            "team": "BOS",
            "margin": 12.0,
            "players": {1628369, 1627759},
        },
        {
            "game_id": "0002",
            "team": "BOS",
            "margin": -5.0,
            "players": {1628369, 1628401},
        },
    ]

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
