from __future__ import annotations

import pytest

from wowy.regression_analysis import fit_player_regression
from wowy.regression_types import RegressionObservation


def test_fit_player_regression_returns_expected_coefficients():
    observations = [
        RegressionObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
        ),
        RegressionObservation(
            "2",
            "2023-24",
            "2024-04-03",
            "NYK",
            "BOS",
            -2.0,
            {202: 1.0, 102: -1.0},
        ),
        RegressionObservation(
            "3",
            "2023-24",
            "2024-04-05",
            "BOS",
            "LAL",
            0.0,
            {101: 1.0, 102: 1.0, 201: -1.0, 202: -1.0},
        ),
    ]

    result = fit_player_regression(
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


def test_fit_player_regression_applies_min_games_filter():
    observations = [
        RegressionObservation(
            "1",
            "2023-24",
            "2024-04-01",
            "BOS",
            "MIL",
            2.0,
            {101: 1.0, 201: -1.0},
        ),
        RegressionObservation(
            "2",
            "2023-24",
            "2024-04-03",
            "NYK",
            "BOS",
            -2.0,
            {202: 1.0, 102: -1.0},
        ),
        RegressionObservation(
            "3",
            "2023-24",
            "2024-04-05",
            "BOS",
            "LAL",
            0.0,
            {101: 1.0, 102: 1.0, 201: -1.0, 202: -1.0},
        ),
    ]

    result = fit_player_regression(
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


def test_fit_player_regression_rejects_singular_system_without_ridge():
    observations = [
        RegressionObservation(
            "1", "2023-24", "2024-04-01", "BOS", "MIL", 2.0, {101: 1.0}
        ),
        RegressionObservation(
            "2", "2023-24", "2024-04-03", "BOS", "NYK", 3.0, {101: 1.0}
        ),
    ]

    with pytest.raises(ValueError, match="singular"):
        fit_player_regression(
            observations,
            player_names={101: "Player 101"},
            ridge_alpha=0.0,
        )


def test_fit_player_regression_handles_singular_system_with_ridge():
    observations = [
        RegressionObservation(
            "1", "2023-24", "2024-04-01", "BOS", "MIL", 2.0, {101: 1.0, 201: -1.0}
        ),
        RegressionObservation(
            "2", "2023-24", "2024-04-03", "BOS", "NYK", 3.0, {101: 1.0, 201: -1.0}
        ),
    ]

    result = fit_player_regression(
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
