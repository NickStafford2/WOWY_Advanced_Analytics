from __future__ import annotations

from wowy.analysis import compute_wowy, filter_results
from wowy.types import GameRecord, PlayerStats


def test_compute_wowy_basic():
    games: list[GameRecord] = [
        GameRecord("1", "team_1", 10.0, {101, 102, 103}),
        GameRecord("2", "team_1", 0.0, {102, 103, 104}),
        GameRecord("3", "team_1", -10.0, {103, 104, 105}),
    ]

    results = compute_wowy(games)

    assert results[101].games_with == 1
    assert results[101].games_without == 2
    assert results[101].avg_margin_with == 10.0
    assert results[101].avg_margin_without == -5.0
    assert results[101].wowy_score == 15.0


def test_filter_results():
    results: dict[int, PlayerStats] = {
        101: PlayerStats(3, 3, 5.0, 1.0, 4.0),
        102: PlayerStats(1, 5, 2.0, 0.0, 2.0),
        103: PlayerStats(4, 1, 1.0, -1.0, 2.0),
    }

    filtered = filter_results(results, min_games_with=2, min_games_without=2)

    assert 101 in filtered
    assert 102 not in filtered
    assert 103 not in filtered
