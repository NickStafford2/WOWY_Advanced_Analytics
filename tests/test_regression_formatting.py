from __future__ import annotations

from wowy.regression_formatting import format_regression_results
from wowy.regression_types import (
    RegressionPlayerEstimate,
    RegressionResult,
)


def test_format_regression_results_contains_expected_text():
    result = RegressionResult(
        observations=3,
        players=2,
        intercept=0.0,
        home_court_advantage=2.5,
        estimates=[
            RegressionPlayerEstimate(101, "Player 101", 2, 31.5, 63.0, 2.0),
            RegressionPlayerEstimate(102, "Player 102", 2, 18.0, 36.0, -2.0),
        ],
    )

    output = format_regression_results(result)

    assert "Regression results (Game-level player model)" in output
    assert "observations=3 players=2 intercept=0.0000 home_court=2.5000" in output
    assert "Player 101" in output
    assert "31.5" in output
    assert "63.0" in output
    assert "2.0000" in output
