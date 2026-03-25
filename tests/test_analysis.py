from __future__ import annotations

from pathlib import Path

from tests.support import (
    game,
    player,
    seed_db_from_team_seasons,
)
from rawr_analytics.metrics.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    apply_wowy_shrinkage,
    compute_wowy,
    compute_wowy_shrinkage_score,
    filter_results,
)
from rawr_analytics.metrics.wowy.models import (
    WowyGameRecord,
    WowyPlayerSeasonRecord,
    WowyPlayerStats,
)
from rawr_analytics.metrics.wowy.records import (
    available_wowy_seasons,
    build_wowy_player_season_records,
    build_wowy_span_chart_rows,
    prepare_wowy_player_season_records,
    serialize_wowy_player_season_records,
)


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


def test_compute_wowy_shrinkage_score_penalizes_imbalanced_samples():
    raw_score = 12.0

    wowy_shrunk_score = compute_wowy_shrinkage_score(
        games_with=2,
        games_without=1,
        wowy_score=raw_score,
    )

    assert wowy_shrunk_score is not None
    assert round(wowy_shrunk_score, 4) == round(
        raw_score * ((4.0 / 3.0) / ((4.0 / 3.0) + DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES)),
        4,
    )


def test_apply_wowy_shrinkage_preserves_context_and_shrinks_score_only():
    results = {
        101: WowyPlayerStats(
            games_with=4,
            games_without=2,
            avg_margin_with=6.0,
            avg_margin_without=-2.0,
            wowy_score=8.0,
            average_minutes=33.5,
            total_minutes=134.0,
        )
    }

    wowy_shrunk_results = apply_wowy_shrinkage(results)

    assert wowy_shrunk_results[101].games_with == 4
    assert wowy_shrunk_results[101].games_without == 2
    assert wowy_shrunk_results[101].avg_margin_with == 6.0
    assert wowy_shrunk_results[101].avg_margin_without == -2.0
    assert wowy_shrunk_results[101].average_minutes == 33.5
    assert wowy_shrunk_results[101].total_minutes == 134.0
    assert wowy_shrunk_results[101].wowy_score is not None
    assert 0.0 < wowy_shrunk_results[101].wowy_score < 8.0


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


def test_serialize_wowy_player_season_records_returns_json_ready_rows():
    records = [
        WowyPlayerSeasonRecord(
            season="2023-24",
            player_id=101,
            player_name="Player 101",
            games_with=2,
            games_without=1,
            avg_margin_with=3.0,
            avg_margin_without=1.0,
            wowy_score=2.0,
            average_minutes=34.0,
            total_minutes=68.0,
        )
    ]

    assert serialize_wowy_player_season_records(records) == [
        {
            "season": "2023-24",
            "player_id": 101,
            "player_name": "Player 101",
            "games_with": 2,
            "games_without": 1,
            "avg_margin_with": 3.0,
            "avg_margin_without": 1.0,
            "wowy_score": 2.0,
            "average_minutes": 34.0,
            "total_minutes": 68.0,
        }
    ]


def test_prepare_wowy_player_season_records_builds_web_ready_rows_from_cache(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(
        db_path,
        [
            (
                "BOS",
                "2022-23",
                [
                    game("1", "2022-23", "2023-04-01", "BOS", "MIL", True, 10.0),
                    game("2", "2022-23", "2023-04-03", "BOS", "NYK", False, -5.0),
                    game("3", "2022-23", "2023-04-05", "BOS", "LAL", True, 4.0),
                ],
                [
                    player("1", "BOS", 101, "Player 101", True, 34.0),
                    player("1", "BOS", 102, "Player 102", True, 31.0),
                    player("2", "BOS", 102, "Player 102", True, 31.0),
                    player("3", "BOS", 101, "Player 101", True, 34.0),
                ],
            ),
            (
                "BOS",
                "2023-24",
                [
                    game("4", "2023-24", "2024-04-01", "BOS", "MIL", True, 8.0),
                    game("5", "2023-24", "2024-04-03", "BOS", "NYK", False, -2.0),
                    game("6", "2023-24", "2024-04-05", "BOS", "LAL", True, 1.0),
                ],
                [
                    player("4", "BOS", 101, "Player 101", True, 35.0),
                    player("4", "BOS", 103, "Player 103", True, 30.0),
                    player("5", "BOS", 101, "Player 101", True, 33.0),
                    player("6", "BOS", 103, "Player 103", True, 30.0),
                ],
            ),
        ],
    )

    records = prepare_wowy_player_season_records(
        teams=["BOS"],
        seasons=None,
        season_type="Regular Season",
        player_metrics_db_path=db_path,
        min_games_with=1,
        min_games_without=1,
        min_average_minutes=0.0,
        min_total_minutes=0.0,
    )

    assert [(record.season, record.player_id) for record in records] == [
        ("2022-23", 101),
        ("2022-23", 102),
        ("2023-24", 103),
        ("2023-24", 101),
    ]
    assert serialize_wowy_player_season_records(records)[0] == {
        "season": "2022-23",
        "player_id": 101,
        "player_name": "Player 101",
        "games_with": 2,
        "games_without": 1,
        "avg_margin_with": 7.0,
        "avg_margin_without": -5.0,
        "wowy_score": 12.0,
        "average_minutes": 34.0,
        "total_minutes": 68.0,
    }


def test_prepare_wowy_player_season_records_uses_db_without_file_fixture_dirs(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(
        db_path,
        [
            (
                "BOS",
                "2022-23",
                [
                    game("1", "2022-23", "2023-04-01", "BOS", "MIL", True, 10.0),
                    game("2", "2022-23", "2023-04-03", "BOS", "NYK", False, -5.0),
                    game("3", "2022-23", "2023-04-05", "BOS", "LAL", True, 4.0),
                ],
                [
                    player("1", "BOS", 101, "Player 101", True, 34.0),
                    player("1", "BOS", 102, "Player 102", True, 31.0),
                    player("2", "BOS", 102, "Player 102", True, 31.0),
                    player("3", "BOS", 101, "Player 101", True, 34.0),
                ],
            )
        ],
    )

    records = prepare_wowy_player_season_records(
        teams=["BOS"],
        seasons=None,
        season_type="Regular Season",
        player_metrics_db_path=db_path,
        min_games_with=1,
        min_games_without=1,
        min_average_minutes=0.0,
        min_total_minutes=0.0,
    )

    assert [(record.season, record.player_id) for record in records] == [
        ("2022-23", 101),
        ("2022-23", 102),
    ]


def test_build_wowy_span_chart_rows_ranks_players_across_selected_seasons():
    records = [
        WowyPlayerSeasonRecord("2022-23", 101, "Player 101", 2, 1, 7.0, -5.0, 12.0, 34.0, 68.0),
        WowyPlayerSeasonRecord("2022-23", 102, "Player 102", 2, 1, 2.5, 4.0, -1.5, 31.0, 62.0),
        WowyPlayerSeasonRecord("2023-24", 101, "Player 101", 2, 1, 3.0, 1.0, 2.0, 34.0, 68.0),
        WowyPlayerSeasonRecord("2023-24", 103, "Player 103", 2, 1, 4.5, -2.0, 6.5, 30.0, 60.0),
    ]

    assert available_wowy_seasons(records) == ["2022-23", "2023-24"]
    assert build_wowy_span_chart_rows(
        records,
        start_season="2022-23",
        end_season="2023-24",
        top_n=2,
    ) == [
        {
            "player_id": 101,
            "player_name": "Player 101",
            "span_average_value": 7.0,
            "season_count": 2,
            "points": [
                {"season": "2022-23", "value": 12.0},
                {"season": "2023-24", "value": 2.0},
            ],
        },
        {
            "player_id": 103,
            "player_name": "Player 103",
            "span_average_value": 3.25,
            "season_count": 1,
            "points": [
                {"season": "2022-23", "value": None},
                {"season": "2023-24", "value": 6.5},
            ],
        },
    ]
