from __future__ import annotations

from wowy.apps.wowy.service import build_wowy_player_season_records
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


def test_build_wowy_player_season_records_returns_one_row_per_player_per_season():
    games: list[WowyGameRecord] = [
        WowyGameRecord("1", "2022-23", "BOS", 10.0, {101, 102}),
        WowyGameRecord("2", "2022-23", "BOS", -5.0, {102}),
        WowyGameRecord("3", "2022-23", "BOS", 4.0, {101}),
        WowyGameRecord("4", "2023-24", "BOS", 8.0, {101, 103}),
        WowyGameRecord("5", "2023-24", "BOS", -2.0, {101}),
        WowyGameRecord("6", "2023-24", "BOS", 1.0, {103}),
    ]

    records = build_wowy_player_season_records(
        games,
        min_games_with=1,
        min_games_without=1,
        player_names={101: "Player 101", 102: "Player 102", 103: "Player 103"},
        player_season_minute_stats={
            ("2022-23", 101): (34.0, 68.0),
            ("2022-23", 102): (31.0, 62.0),
            ("2023-24", 101): (34.0, 68.0),
            ("2023-24", 103): (30.0, 60.0),
        },
        min_average_minutes=0.0,
        min_total_minutes=0.0,
    )

    assert [(record.season, record.player_id) for record in records] == [
        ("2022-23", 101),
        ("2022-23", 102),
        ("2023-24", 103),
        ("2023-24", 101),
    ]
    assert records[0].player_name == "Player 101"
    assert records[0].wowy_score == 12.0
    assert records[2].total_minutes == 60.0
    assert records[3].average_minutes == 34.0
