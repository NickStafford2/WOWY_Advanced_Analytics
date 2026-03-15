from __future__ import annotations

from wowy.analysis import compute_wowy, filter_results
from wowy.types import GameRecord, PlayerStats


def test_compute_wowy_basic():
    games: list[GameRecord] = [
        {
            "game_id": "1",
            "team": "team_1",
            "margin": 10.0,
            "players": {"player_A", "player_B", "player_C"},
        },
        {
            "game_id": "2",
            "team": "team_1",
            "margin": 0.0,
            "players": {"player_B", "player_C", "player_D"},
        },
        {
            "game_id": "3",
            "team": "team_1",
            "margin": -10.0,
            "players": {"player_C", "player_D", "player_E"},
        },
    ]

    results = compute_wowy(games)

    assert results["player_A"]["games_with"] == 1
    assert results["player_A"]["games_without"] == 2
    assert results["player_A"]["avg_margin_with"] == 10.0
    assert results["player_A"]["avg_margin_without"] == -5.0
    assert results["player_A"]["wowy_score"] == 15.0


def test_filter_results():
    results: dict[str, PlayerStats] = {
        "player_A": {
            "games_with": 3,
            "games_without": 3,
            "avg_margin_with": 5.0,
            "avg_margin_without": 1.0,
            "wowy_score": 4.0,
        },
        "player_B": {
            "games_with": 1,
            "games_without": 5,
            "avg_margin_with": 2.0,
            "avg_margin_without": 0.0,
            "wowy_score": 2.0,
        },
        "player_C": {
            "games_with": 4,
            "games_without": 1,
            "avg_margin_with": 1.0,
            "avg_margin_without": -1.0,
            "wowy_score": 2.0,
        },
    }

    filtered = filter_results(results, min_games_with=2, min_games_without=2)

    assert "player_A" in filtered
    assert "player_B" not in filtered
    assert "player_C" not in filtered
