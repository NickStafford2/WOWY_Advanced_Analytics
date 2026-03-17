from __future__ import annotations

import pytest

from wowy.apps.rawr.data import build_rawr_observations, count_player_games
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord


def test_build_rawr_observations_preserves_game_context():
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
        NormalizedGamePlayerRecord("1", "BOS", 101, "Player 101", True, 30.0),
        NormalizedGamePlayerRecord("1", "BOS", 102, "Player 102", False, None),
        NormalizedGamePlayerRecord("1", "BOS", 103, "Player 103", True, 18.0),
        NormalizedGamePlayerRecord("1", "MIL", 201, "Player 201", True, 24.0),
        NormalizedGamePlayerRecord("1", "MIL", 202, "Player 202", True, 24.0),
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
        NormalizedGamePlayerRecord("1", "BOS", 101, "Player 101", True, 32.0),
        NormalizedGamePlayerRecord("1", "BOS", 103, "Player 103", True, 16.0),
        NormalizedGamePlayerRecord("1", "MIL", 201, "Player 201", True, 48.0),
        NormalizedGamePlayerRecord("2", "BOS", 101, "Player 101", True, 48.0),
        NormalizedGamePlayerRecord("2", "NYK", 202, "Player 202", True, 48.0),
    ]

    observations, _ = build_rawr_observations(games, game_players)

    assert count_player_games(observations) == {101: 2, 103: 1, 201: 1, 202: 1}


def test_build_rawr_observations_rejects_missing_minutes_for_appeared_player():
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
    ]
    game_players = [
        NormalizedGamePlayerRecord("1", "BOS", 101, "Player 101", True, None),
        NormalizedGamePlayerRecord("1", "MIL", 201, "Player 201", True, 48.0),
    ]

    with pytest.raises(ValueError, match="Missing positive minutes"):
        build_rawr_observations(games, game_players)
