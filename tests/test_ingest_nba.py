from __future__ import annotations

from pathlib import Path

import pandas as pd

from wowy.ingest_nba import write_team_season_games_csv
from wowy.io import load_games_from_csv


def test_write_team_season_games_csv_uses_existing_games_shape(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        "wowy.ingest_nba.teams.find_team_by_abbreviation",
        lambda abbreviation: {"id": 1610612738, "abbreviation": "BOS"},
    )

    class FakeLeagueGameFinder:
        def __init__(self, **kwargs):
            assert kwargs["team_id_nullable"] == 1610612738
            assert kwargs["season_nullable"] == "2023-24"
            assert kwargs["season_type_nullable"] == "Regular Season"

        def get_data_frames(self):
            return [pd.DataFrame({"GAME_ID": ["0001", "0001", "0002"]})]

    class FakeBoxScoreTraditionalV2:
        def __init__(self, game_id: str):
            self.game_id = game_id

        def get_data_frames(self):
            if self.game_id == "0001":
                return [
                    pd.DataFrame(
                        {
                            "TEAM_ABBREVIATION": ["BOS", "BOS", "BOS", "LAL"],
                            "PLAYER_NAME": [
                                "Jayson Tatum",
                                "Jaylen Brown",
                                "Deep Bench",
                                "Opponent Player",
                            ],
                            "MIN": ["35:12", "34:01", "0:00", "33:44"],
                        }
                    ),
                    pd.DataFrame(
                        {
                            "TEAM_ABBREVIATION": ["BOS", "LAL"],
                            "PLUS_MINUS": [12, -12],
                        }
                    ),
                ]

            return [
                pd.DataFrame(
                    {
                        "TEAM_ABBREVIATION": ["BOS", "BOS", "LAL"],
                        "PLAYER_NAME": [
                            "Jayson Tatum",
                            "Derrick White",
                            "Opponent Player",
                        ],
                        "MIN": ["36:00", "30:15", "31:02"],
                    }
                ),
                pd.DataFrame(
                    {
                        "TEAM_ABBREVIATION": ["BOS", "LAL"],
                        "PLUS_MINUS": [-5, 5],
                    }
                ),
            ]

    monkeypatch.setattr(
        "wowy.ingest_nba.leaguegamefinder.LeagueGameFinder",
        FakeLeagueGameFinder,
    )
    monkeypatch.setattr(
        "wowy.ingest_nba.boxscoretraditionalv2.BoxScoreTraditionalV2",
        FakeBoxScoreTraditionalV2,
    )

    csv_path = tmp_path / "games.csv"
    write_team_season_games_csv("BOS", "2023-24", csv_path)

    games = load_games_from_csv(csv_path)

    assert games == [
        {
            "game_id": "0001",
            "team": "BOS",
            "margin": 12.0,
            "players": {"Jayson Tatum", "Jaylen Brown"},
        },
        {
            "game_id": "0002",
            "team": "BOS",
            "margin": -5.0,
            "players": {"Jayson Tatum", "Derrick White"},
        },
    ]
