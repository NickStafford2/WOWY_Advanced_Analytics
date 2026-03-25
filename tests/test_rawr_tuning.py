from __future__ import annotations

from argparse import Namespace

from wowy.metrics.rawr.models import RawrPlayerSeasonRecord
from wowy.metrics.rawr.tuning import (
    aggregate_rawr_training_records,
    aggregate_wowy_training_records,
    count_evaluation_steps,
    evaluate_configs,
    format_results_table,
    parse_float_grid,
)
from wowy.metrics.wowy.models import WowyPlayerSeasonRecord


def test_parse_float_grid_parses_values():
    assert parse_float_grid("1, 3,10.5") == [1.0, 3.0, 10.5]


def test_count_evaluation_steps_accounts_for_minute_scale_grid():
    args = Namespace(
        rawr_ridge_values=[1.0, 3.0],
        shrinkage_mode=["uniform", "minutes"],
        shrinkage_strength_values=[0.0, 1.0],
        shrinkage_minute_scale_values=[48.0, 240.0, 480.0],
    )

    assert count_evaluation_steps(args) == 19


def test_aggregate_training_records_support_latest():
    wowy_records = [
        WowyPlayerSeasonRecord("2021-22", 101, "Player 101", 20, 5, 5.0, 1.0, 4.0, 34.0, 680.0),
        WowyPlayerSeasonRecord("2022-23", 101, "Player 101", 20, 5, 7.0, 1.0, 6.0, 35.0, 700.0),
    ]
    rawr_records = [
        RawrPlayerSeasonRecord("2021-22", 101, "Player 101", 40, 34.0, 1360.0, 1.0),
        RawrPlayerSeasonRecord("2022-23", 101, "Player 101", 42, 35.0, 1470.0, 3.0),
    ]

    assert aggregate_wowy_training_records(wowy_records, "latest")[101].value == 6.0
    assert aggregate_rawr_training_records(rawr_records, "latest")[101].value == 3.0


def test_evaluate_configs_compares_wowy_and_rawr(monkeypatch):
    def fake_prepare_wowy_player_season_records(*, seasons, **_kwargs):
        if seasons == ["2023-24"]:
            return [
                WowyPlayerSeasonRecord(
                    "2023-24", 101, "Player 101", 20, 5, 0.0, 0.0, 10.0, 34.0, 680.0
                ),
                WowyPlayerSeasonRecord(
                    "2023-24", 102, "Player 102", 20, 5, 0.0, 0.0, 5.0, 33.0, 660.0
                ),
            ]
        return [
            WowyPlayerSeasonRecord("2021-22", 101, "Player 101", 20, 5, 0.0, 0.0, 9.0, 34.0, 680.0),
            WowyPlayerSeasonRecord(
                "2022-23", 101, "Player 101", 20, 5, 0.0, 0.0, 11.0, 35.0, 700.0
            ),
            WowyPlayerSeasonRecord("2021-22", 102, "Player 102", 20, 5, 0.0, 0.0, 3.0, 33.0, 660.0),
            WowyPlayerSeasonRecord("2022-23", 102, "Player 102", 20, 5, 0.0, 0.0, 4.0, 32.0, 640.0),
        ]

    def fake_prepare_rawr_player_season_records(
        *,
        ridge_alpha,
        shrinkage_mode,
        shrinkage_strength,
        shrinkage_minute_scale,
        **_kwargs,
    ):
        if shrinkage_mode == "minutes":
            player_101_score = (
                8.0 + ridge_alpha + shrinkage_strength + (shrinkage_minute_scale / 1000.0)
            )
        else:
            player_101_score = 8.0
        return [
            RawrPlayerSeasonRecord(
                "2021-22", 101, "Player 101", 40, 34.0, 1360.0, player_101_score
            ),
            RawrPlayerSeasonRecord(
                "2022-23",
                101,
                "Player 101",
                42,
                35.0,
                1470.0,
                player_101_score + 1.0,
            ),
            RawrPlayerSeasonRecord("2021-22", 102, "Player 102", 38, 33.0, 1254.0, 2.0),
            RawrPlayerSeasonRecord("2022-23", 102, "Player 102", 39, 32.0, 1248.0, 3.0),
        ]

    monkeypatch.setattr(
        "wowy.metrics.rawr.tuning.prepare_wowy_player_season_records",
        fake_prepare_wowy_player_season_records,
    )
    monkeypatch.setattr(
        "wowy.metrics.rawr.tuning.prepare_rawr_player_season_records",
        fake_prepare_rawr_player_season_records,
    )

    args = Namespace(
        team=None,
        train_season=["2021-22", "2022-23"],
        holdout_season="2023-24",
        season_type="Regular Season",
        aggregation="mean",
        rawr_ridge_values=[1.0],
        shrinkage_mode=["uniform", "minutes"],
        shrinkage_strength_values=[0.5],
        shrinkage_minute_scale_values=[48.0],
        rawr_min_games=35,
        holdout_min_games_with=15,
        holdout_min_games_without=2,
        min_average_minutes=30.0,
        min_total_minutes=600.0,
        top_n=2,
        source_data_dir=None,
    )

    results = evaluate_configs(args)

    assert [result.model for result in results].count("wowy-baseline") == 1
    assert [result.model for result in results].count("rawr") == 2
    table = format_results_table(results)
    assert "RAWR tuning comparison" in table
    assert "wowy-baseline" in table
    assert "minutes" in table
