from __future__ import annotations

import pytest

from wowy.metrics.rawr.analysis import (
    build_player_penalties,
    count_player_season_minutes,
    fit_player_rawr,
    tune_ridge_alpha,
)
from wowy.metrics.rawr.models import RawrObservation


def test_fit_player_rawr_returns_expected_coefficients():
    observations = [
        RawrObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
        ),
        RawrObservation(
            "2",
            "2023-24",
            "2024-04-03",
            "NYK",
            "BOS",
            -2.0,
            {202: 1.0, 102: -1.0},
        ),
        RawrObservation(
            "3",
            "2023-24",
            "2024-04-05",
            "BOS",
            "LAL",
            0.0,
            {101: 1.0, 102: 1.0, 201: -1.0, 202: -1.0},
        ),
    ]

    result = fit_player_rawr(
        observations,
        player_names={
            101: "Player 101",
            102: "Player 102",
            201: "Player 201",
            202: "Player 202",
        },
        ridge_alpha=1.0,
    )

    estimates = {estimate.player_id: estimate for estimate in result.estimates}
    assert result.observations == 3
    assert result.players == 4
    assert result.intercept == pytest.approx(0.0)
    assert result.home_court_advantage != 0.0
    assert estimates[101].coefficient == pytest.approx(-estimates[201].coefficient)
    assert estimates[102].coefficient == pytest.approx(-estimates[202].coefficient)
    assert estimates[101].average_minutes is None
    assert estimates[101].total_minutes is None


def test_fit_player_rawr_does_not_use_minute_thresholds_for_prefit():
    observations = [
        RawrObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
        ),
        RawrObservation(
            "2",
            "2023-24",
            "2024-04-03",
            "BOS",
            "NYK",
            -2.0,
            {101: 1.0, 202: -1.0},
        ),
    ]

    result = fit_player_rawr(
        observations,
        player_names={
            101: "Player 101",
            201: "Player 201",
            202: "Player 202",
        },
        min_games=1,
        ridge_alpha=1.0,
    )

    assert {estimate.player_id for estimate in result.estimates} == {101, 201, 202}


def test_fit_player_rawr_applies_min_games_filter():
    observations = [
        RawrObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
        ),
        RawrObservation(
            "2",
            "2023-24",
            "2024-04-03",
            "NYK",
            "BOS",
            -2.0,
            {202: 1.0, 102: -1.0},
        ),
        RawrObservation(
            "3",
            "2023-24",
            "2024-04-05",
            "BOS",
            "LAL",
            0.0,
            {101: 1.0, 102: 1.0, 201: -1.0, 202: -1.0},
        ),
    ]

    result = fit_player_rawr(
        observations,
        player_names={
            101: "Player 101",
            102: "Player 102",
            201: "Player 201",
            202: "Player 202",
        },
        min_games=2,
        ridge_alpha=1.0,
    )

    assert [estimate.player_id for estimate in result.estimates] == [101, 102, 201, 202]


def test_fit_player_rawr_uses_separate_coefficients_per_player_season():
    observations = [
        RawrObservation(
            "1",
            "2022-23",
            "2023-04-01",
            "BOS",
            "MIL",
            6.0,
            {101: 1.0, 201: -1.0},
        ),
        RawrObservation(
            "2",
            "2022-23",
            "2023-04-03",
            "BOS",
            "NYK",
            5.0,
            {101: 1.0, 202: -1.0},
        ),
        RawrObservation(
            "3",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            -6.0,
            {101: 1.0, 201: -1.0},
        ),
        RawrObservation(
            "4",
            "2023-24",
            "2024-04-03",
            "BOS",
            "NYK",
            -5.0,
            {101: 1.0, 202: -1.0},
        ),
    ]

    result = fit_player_rawr(
        observations,
        player_names={
            101: "Player 101",
            201: "Player 201",
            202: "Player 202",
        },
        min_games=2,
        ridge_alpha=1.0,
    )

    estimates = {(estimate.season, estimate.player_id): estimate for estimate in result.estimates}

    assert ("2022-23", 101) in estimates
    assert ("2023-24", 101) in estimates
    assert estimates[("2022-23", 101)].coefficient > 0.0
    assert estimates[("2023-24", 101)].coefficient < 0.0


def test_build_player_penalties_supports_game_count_shrinkage():
    observations = [
        RawrObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
        ),
        RawrObservation(
            "2",
            "2023-24",
            "2024-04-03",
            "BOS",
            "NYK",
            -2.0,
            {101: 1.0, 202: -1.0},
        ),
    ]

    penalties = build_player_penalties(
        observations=observations,
        player_keys=[("2023-24", 101), ("2023-24", 201)],
        ridge_alpha=10.0,
        shrinkage_mode="game-count",
        shrinkage_strength=1.0,
    )

    assert penalties[("2023-24", 101)] == pytest.approx(5.0)
    assert penalties[("2023-24", 201)] == pytest.approx(10.0)


def test_count_player_season_minutes_aggregates_minutes():
    observations = [
        RawrObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
            {101: 30.0, 201: 40.0},
        ),
        RawrObservation(
            "2",
            "2023-24",
            "2024-04-03",
            "BOS",
            "NYK",
            -2.0,
            {101: 1.0, 202: -1.0},
            {101: 10.0, 202: 48.0},
        ),
    ]

    assert count_player_season_minutes(observations) == {
        ("2023-24", 101): 40.0,
        ("2023-24", 201): 40.0,
        ("2023-24", 202): 48.0,
    }


def test_build_player_penalties_supports_minute_shrinkage():
    observations = [
        RawrObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
            {101: 5.0, 201: 40.0},
        ),
        RawrObservation(
            "2",
            "2023-24",
            "2024-04-03",
            "BOS",
            "NYK",
            -2.0,
            {101: 1.0, 202: -1.0},
            {101: 5.0, 202: 35.0},
        ),
    ]

    penalties = build_player_penalties(
        observations=observations,
        player_keys=[("2023-24", 101), ("2023-24", 201)],
        ridge_alpha=10.0,
        shrinkage_mode="minutes",
        shrinkage_strength=1.0,
        shrinkage_minute_scale=48.0,
    )

    assert penalties[("2023-24", 101)] == pytest.approx(48.0)
    assert penalties[("2023-24", 201)] == pytest.approx(12.0)
    assert penalties[("2023-24", 101)] > penalties[("2023-24", 201)]


def test_fit_player_rawr_rejects_invalid_shrinkage_mode():
    observations = [
        RawrObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
        ),
    ]

    with pytest.raises(ValueError, match="Shrinkage mode"):
        fit_player_rawr(
            observations,
            player_names={101: "Player 101", 201: "Player 201"},
            ridge_alpha=1.0,
            shrinkage_mode="bad-mode",
        )


def test_fit_player_rawr_rejects_non_positive_minute_scale():
    observations = [
        RawrObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
        ),
    ]

    with pytest.raises(ValueError, match="minute scale"):
        fit_player_rawr(
            observations,
            player_names={101: "Player 101", 201: "Player 201"},
            ridge_alpha=1.0,
            shrinkage_minute_scale=0.0,
        )


def test_fit_player_rawr_rejects_singular_system_without_ridge():
    observations = [
        RawrObservation("1", "2023-24", "2024-04-01", "BOS", "MIL", 2.0, {101: 1.0}),
        RawrObservation("2", "2023-24", "2024-04-03", "BOS", "NYK", 3.0, {101: 1.0}),
    ]

    with pytest.raises(ValueError, match="singular"):
        fit_player_rawr(
            observations,
            player_names={101: "Player 101"},
            ridge_alpha=0.0,
        )


def test_fit_player_rawr_handles_singular_system_with_ridge():
    observations = [
        RawrObservation("1", "2023-24", "2024-04-01", "BOS", "MIL", 2.0, {101: 1.0, 201: -1.0}),
        RawrObservation("2", "2023-24", "2024-04-03", "BOS", "NYK", 3.0, {101: 1.0, 201: -1.0}),
    ]

    result = fit_player_rawr(
        observations,
        player_names={101: "Player 101", 201: "Player 201"},
        ridge_alpha=1.0,
    )

    assert result.players == 2
    assert result.intercept == pytest.approx(0.0)
    assert result.home_court_advantage == pytest.approx(2.5)
    assert result.estimates[0].player_id == 101
    assert result.estimates[0].coefficient == pytest.approx(0.0)
    assert result.estimates[1].player_id == 201
    assert result.estimates[1].coefficient == pytest.approx(0.0)


def test_tune_ridge_alpha_returns_best_value_from_grid():
    observations = [
        RawrObservation("1", "2023-24", "2024-04-01", "BOS", "MIL", 4.0, {101: 1.0, 201: -1.0}),
        RawrObservation("2", "2023-24", "2024-04-03", "BOS", "MIL", 3.0, {101: 1.0, 201: -1.0}),
        RawrObservation("3", "2023-24", "2024-04-05", "BOS", "MIL", 5.0, {101: 1.0, 201: -1.0}),
        RawrObservation("4", "2023-24", "2024-04-07", "BOS", "MIL", 4.0, {101: 1.0, 201: -1.0}),
        RawrObservation("5", "2023-24", "2024-04-09", "BOS", "MIL", 4.5, {101: 1.0, 201: -1.0}),
    ]

    summary = tune_ridge_alpha(
        observations,
        player_names={101: "Player 101", 201: "Player 201"},
        alphas=[0.1, 1.0, 10.0],
        min_games=1,
        validation_fraction=0.2,
    )

    assert summary.best_alpha in {0.1, 1.0, 10.0}
    assert [result.alpha for result in summary.results] == [0.1, 1.0, 10.0]
    assert all(result.validation_mse >= 0.0 for result in summary.results)
