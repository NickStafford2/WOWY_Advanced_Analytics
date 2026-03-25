from __future__ import annotations

import pytest

from tests.support import game, player
from wowy.apps.rawr.data import count_player_games
from wowy.apps.rawr.inputs import build_rawr_observations


def test_build_rawr_observations_preserves_game_context():
    games = [
        game("1", "2023-24", "2024-04-01", "BOS", "MIL", True, 10.0),
        game("1", "2023-24", "2024-04-01", "MIL", "BOS", False, -10.0),
    ]
    game_players = [
        player("1", "BOS", 101, "Player 101", True, 30.0),
        player("1", "BOS", 102, "Player 102", False, None),
        player("1", "BOS", 103, "Player 103", True, 18.0),
        player("1", "MIL", 201, "Player 201", True, 24.0),
        player("1", "MIL", 202, "Player 202", True, 24.0),
    ]

    observations, player_names = build_rawr_observations(games, game_players)

    assert observations[0].home_team == "BOS"
    assert observations[0].away_team == "MIL"
    assert observations[0].margin == 10.0
    assert observations[0].player_weights == pytest.approx(
        {
            101: 30.0 / 48.0 * 5.0,
            103: 18.0 / 48.0 * 5.0,
            201: -(24.0 / 48.0 * 5.0),
            202: -(24.0 / 48.0 * 5.0),
        }
    )
    assert player_names == {
        101: "Player 101",
        102: "Player 102",
        103: "Player 103",
        201: "Player 201",
        202: "Player 202",
    }


def test_count_player_games_counts_appeared_games():
    games = [
        game("1", "2023-24", "2024-04-01", "BOS", "MIL", True, 10.0),
        game("1", "2023-24", "2024-04-01", "MIL", "BOS", False, -10.0),
        game("2", "2023-24", "2024-04-03", "BOS", "NYK", False, -3.0),
        game("2", "2023-24", "2024-04-03", "NYK", "BOS", True, 3.0),
    ]
    game_players = [
        player("1", "BOS", 101, "Player 101", True, 32.0),
        player("1", "BOS", 103, "Player 103", True, 16.0),
        player("1", "MIL", 201, "Player 201", True, 48.0),
        player("2", "BOS", 101, "Player 101", True, 48.0),
        player("2", "NYK", 202, "Player 202", True, 48.0),
    ]

    observations, _ = build_rawr_observations(games, game_players)

    assert count_player_games(observations) == {101: 2, 103: 1, 201: 1, 202: 1}


def test_build_rawr_observations_rejects_missing_minutes_for_appeared_player():
    games = [
        game("1", "2023-24", "2024-04-01", "BOS", "MIL", True, 10.0),
        game("1", "2023-24", "2024-04-01", "MIL", "BOS", False, -10.0),
    ]
    game_players = [
        player("1", "BOS", 101, "Player 101", True, None),
        player("1", "MIL", 201, "Player 201", True, 48.0),
    ]

    with pytest.raises(ValueError, match="Missing positive minutes"):
        build_rawr_observations(games, game_players)
