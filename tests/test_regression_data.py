from __future__ import annotations

from wowy.regression_data import build_regression_observations, count_player_games
from wowy.types import NormalizedGamePlayerRecord, NormalizedGameRecord


def test_build_regression_observations_preserves_game_context():
    games = [
        NormalizedGameRecord(
            game_id="1",
            season="2023-24",
            game_date="2024-04-01",
            team="BOS",
            opponent="MIL",
            is_home=True,
            margin=10.0,
            season_type="Regular Season",
            source="nba_api",
        ),
        NormalizedGameRecord(
            game_id="1",
            season="2023-24",
            game_date="2024-04-01",
            team="MIL",
            opponent="BOS",
            is_home=False,
            margin=-10.0,
            season_type="Regular Season",
            source="nba_api",
        )
    ]
    game_players = [
        NormalizedGamePlayerRecord("1", "BOS", 101, "Player 101", True, None),
        NormalizedGamePlayerRecord("1", "BOS", 102, "Player 102", False, None),
        NormalizedGamePlayerRecord("1", "BOS", 103, "Player 103", True, None),
        NormalizedGamePlayerRecord("1", "MIL", 201, "Player 201", True, None),
        NormalizedGamePlayerRecord("1", "MIL", 202, "Player 202", True, None),
    ]

    observations, player_names = build_regression_observations(games, game_players)

    assert observations[0].home_team == "BOS"
    assert observations[0].away_team == "MIL"
    assert observations[0].margin == 10.0
    assert observations[0].player_weights == {
        101: 1.0,
        103: 1.0,
        201: -1.0,
        202: -1.0,
    }
    assert player_names == {
        101: "Player 101",
        102: "Player 102",
        103: "Player 103",
        201: "Player 201",
        202: "Player 202",
    }


def test_count_player_games_counts_appeared_games():
    games = [
        NormalizedGameRecord(
            game_id="1",
            season="2023-24",
            game_date="2024-04-01",
            team="BOS",
            opponent="MIL",
            is_home=True,
            margin=10.0,
            season_type="Regular Season",
            source="nba_api",
        ),
        NormalizedGameRecord(
            game_id="1",
            season="2023-24",
            game_date="2024-04-01",
            team="MIL",
            opponent="BOS",
            is_home=False,
            margin=-10.0,
            season_type="Regular Season",
            source="nba_api",
        ),
        NormalizedGameRecord(
            game_id="2",
            season="2023-24",
            game_date="2024-04-03",
            team="BOS",
            opponent="NYK",
            is_home=False,
            margin=-3.0,
            season_type="Regular Season",
            source="nba_api",
        ),
        NormalizedGameRecord(
            game_id="2",
            season="2023-24",
            game_date="2024-04-03",
            team="NYK",
            opponent="BOS",
            is_home=True,
            margin=3.0,
            season_type="Regular Season",
            source="nba_api",
        ),
    ]
    game_players = [
        NormalizedGamePlayerRecord("1", "BOS", 101, "Player 101", True, None),
        NormalizedGamePlayerRecord("1", "BOS", 103, "Player 103", True, None),
        NormalizedGamePlayerRecord("1", "MIL", 201, "Player 201", True, None),
        NormalizedGamePlayerRecord("2", "BOS", 101, "Player 101", True, None),
        NormalizedGamePlayerRecord("2", "NYK", 202, "Player 202", True, None),
    ]

    observations, _ = build_regression_observations(games, game_players)

    assert count_player_games(observations) == {101: 2, 103: 1, 201: 1, 202: 1}
