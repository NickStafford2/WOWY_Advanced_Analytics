from __future__ import annotations

from wowy.formatting import format_results_table
from wowy.types import PlayerStats


def test_format_results_table_contains_expected_text():
    results: dict[str, PlayerStats] = {
        "player_A": {
            "games_with": 3,
            "games_without": 2,
            "avg_margin_with": 5.0,
            "avg_margin_without": 1.0,
            "wowy_score": 4.0,
        }
    }

    output = format_results_table(results)

    assert "WOWY results (Version 1)" in output
    assert "player_A" in output
    assert "4.00" in output
