from __future__ import annotations

import pytest

from wowy.regression_analysis import fit_player_regression
from wowy.regression_types import RegressionObservation


def test_fit_player_regression_returns_expected_coefficients():
    observations = [
        RegressionObservation("1", "2023-24", "2024-04-01", "BOS", "MIL", True, 2.0, {101}),
        RegressionObservation("2", "2023-24", "2024-04-03", "BOS", "NYK", False, -2.0, {102}),
        RegressionObservation("3", "2023-24", "2024-04-05", "BOS", "LAL", True, 0.0, {101, 102}),
    ]

    result = fit_player_regression(
        observations,
        player_names={101: "Player 101", 102: "Player 102"},
    )

    estimates = {estimate.player_id: estimate for estimate in result.estimates}
    assert result.observations == 3
    assert result.players == 2
    assert result.intercept == pytest.approx(0.0)
    assert estimates[101].coefficient == pytest.approx(2.0)
    assert estimates[102].coefficient == pytest.approx(-2.0)


def test_fit_player_regression_applies_min_games_filter():
    observations = [
        RegressionObservation("1", "2023-24", "2024-04-01", "BOS", "MIL", True, 2.0, {101}),
        RegressionObservation("2", "2023-24", "2024-04-03", "BOS", "NYK", False, -2.0, {102}),
        RegressionObservation("3", "2023-24", "2024-04-05", "BOS", "LAL", True, 0.0, {101, 102}),
    ]

    result = fit_player_regression(
        observations,
        player_names={101: "Player 101", 102: "Player 102"},
        min_games=2,
    )

    assert [estimate.player_id for estimate in result.estimates] == [101, 102]


def test_fit_player_regression_rejects_singular_system():
    observations = [
        RegressionObservation("1", "2023-24", "2024-04-01", "BOS", "MIL", True, 2.0, {101}),
        RegressionObservation("2", "2023-24", "2024-04-03", "BOS", "NYK", False, 3.0, {101}),
    ]

    with pytest.raises(ValueError, match="singular"):
        fit_player_regression(observations, player_names={101: "Player 101"})
