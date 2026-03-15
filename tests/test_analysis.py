from __future__ import annotations

from wowy.analysis import compute_wowy, filter_results
from wowy.types import GameRecord, PlayerStats


def test_compute_wowy_basic():
    games: list[GameRecord] = [
        {
            "game_id": "1",
            "team": "team_1",
            "margin": 10.0,
            "players": {101, 102, 103},
        },
        {
            "game_id": "2",
            "team": "team_1",
            "margin": 0.0,
            "players": {102, 103, 104},
        },
        {
            "game_id": "3",
            "team": "team_1",
            "margin": -10.0,
            "players": {103, 104, 105},
        },
    ]

    results = compute_wowy(games)

    assert results[101]["games_with"] == 1
    assert results[101]["games_without"] == 2
    assert results[101]["avg_margin_with"] == 10.0
    assert results[101]["avg_margin_without"] == -5.0
    assert results[101]["wowy_score"] == 15.0


def test_filter_results():
    results: dict[int, PlayerStats] = {
        101: {
            "games_with": 3,
            "games_without": 3,
            "avg_margin_with": 5.0,
            "avg_margin_without": 1.0,
            "wowy_score": 4.0,
        },
        102: {
            "games_with": 1,
            "games_without": 5,
            "avg_margin_with": 2.0,
            "avg_margin_without": 0.0,
            "wowy_score": 2.0,
        },
        103: {
            "games_with": 4,
            "games_without": 1,
            "avg_margin_with": 1.0,
            "avg_margin_without": -1.0,
            "wowy_score": 2.0,
        },
    }

    filtered = filter_results(results, min_games_with=2, min_games_without=2)

    assert 101 in filtered
    assert 102 not in filtered
    assert 103 not in filtered
