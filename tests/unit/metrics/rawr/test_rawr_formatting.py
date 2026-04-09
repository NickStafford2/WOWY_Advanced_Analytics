from __future__ import annotations

from rawr_analytics.metrics.rawr.formatting import format_rawr_results
from rawr_analytics.metrics.rawr.models import (
    RawrPlayerEstimate,
    RawrResult,
)


def test_format_rawr_results_contains_expected_text():
    result = RawrResult(
        observations=3,
        players=2,
        intercept=0.0,
        home_court_advantage=2.5,
        estimates=[
            RawrPlayerEstimate("2023-24", 101, "Player 101", 2, 31.5, 63.0, 2.0),
            RawrPlayerEstimate("2023-24", 102, "Player 102", 2, 18.0, 36.0, -2.0),
        ],
    )

    output = format_rawr_results(result)

    assert "RAWR results (Game-level player model)" in output
    assert "observations=3 players=2 intercept=0.0000 home_court=2.5000" in output
    assert "season" in output
    assert "2023-24" in output
    assert "Player 101" in output
    assert "31.5" in output
    assert "63.0" in output
    assert "2.0000" in output


def test_format_rawr_results_handles_empty_estimate_list():
    result = RawrResult(
        observations=3,
        players=2,
        intercept=0.0,
        home_court_advantage=2.5,
        estimates=[],
    )

    output = format_rawr_results(result)

    assert "RAWR results (Game-level player model)" in output
    assert "No players matched the current filters." in output
