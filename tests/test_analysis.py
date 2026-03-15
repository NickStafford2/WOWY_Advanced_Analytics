from __future__ import annotations

from wowy.apps.wowy.analysis import compute_wowy, filter_results
from wowy.apps.wowy.models import WowyGameRecord, WowyPlayerStats


def test_compute_wowy_basic():
    games: list[WowyGameRecord] = [
        WowyGameRecord("1", "2023-24", "team_1", 10.0, {101, 102, 103}),
        WowyGameRecord("2", "2023-24", "team_1", 0.0, {102, 103, 104}),
        WowyGameRecord("3", "2023-24", "team_1", -10.0, {103, 104, 105}),
    ]

    results = compute_wowy(games)

    assert results[101].games_with == 1
    assert results[101].games_without == 2
    assert results[101].avg_margin_with == 10.0
    assert results[101].avg_margin_without == -5.0
    assert results[101].wowy_score == 15.0
    assert results[101].average_minutes is None
    assert results[101].total_minutes is None


def test_compute_wowy_limits_without_sample_to_matching_team_season():
    games: list[WowyGameRecord] = [
        WowyGameRecord("1", "2023-24", "team_1", 10.0, {101, 102}),
        WowyGameRecord("2", "2023-24", "team_1", -5.0, {102}),
        WowyGameRecord("3", "2023-24", "team_2", -20.0, {201}),
        WowyGameRecord("4", "2024-25", "team_1", -30.0, {301}),
    ]

    results = compute_wowy(games)

    assert results[101].games_with == 1
    assert results[101].games_without == 1
    assert results[101].avg_margin_with == 10.0
    assert results[101].avg_margin_without == -5.0
    assert results[101].wowy_score == 15.0


def test_filter_results():
    results: dict[int, WowyPlayerStats] = {
        101: WowyPlayerStats(3, 3, 5.0, 1.0, 4.0),
        102: WowyPlayerStats(1, 5, 2.0, 0.0, 2.0),
        103: WowyPlayerStats(4, 1, 1.0, -1.0, 2.0),
    }

    filtered = filter_results(results, min_games_with=2, min_games_without=2)

    assert 101 in filtered
    assert 102 not in filtered
    assert 103 not in filtered
