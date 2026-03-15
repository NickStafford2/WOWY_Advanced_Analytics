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
        )
    ]
    game_players = [
        NormalizedGamePlayerRecord("1", "BOS", 101, "Player 101", True, None),
        NormalizedGamePlayerRecord("1", "BOS", 102, "Player 102", False, None),
        NormalizedGamePlayerRecord("1", "BOS", 103, "Player 103", True, None),
    ]

    observations, player_names = build_regression_observations(games, game_players)

    assert observations[0].opponent == "MIL"
    assert observations[0].is_home is True
    assert observations[0].player_ids == {101, 103}
    assert player_names == {101: "Player 101", 102: "Player 102", 103: "Player 103"}


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
    ]
    game_players = [
        NormalizedGamePlayerRecord("1", "BOS", 101, "Player 101", True, None),
        NormalizedGamePlayerRecord("1", "BOS", 103, "Player 103", True, None),
        NormalizedGamePlayerRecord("2", "BOS", 101, "Player 101", True, None),
    ]

    observations, _ = build_regression_observations(games, game_players)

    assert count_player_games(observations) == {101: 2, 103: 1}
