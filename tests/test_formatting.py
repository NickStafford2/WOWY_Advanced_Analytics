from __future__ import annotations

from wowy.formatting import format_results_table
from wowy.types import PlayerStats


def test_format_results_table_contains_expected_text():
    results: dict[int, PlayerStats] = {
        203999: PlayerStats(3, 2, 5.0, 1.0, 4.0)
    }

    output = format_results_table(results, player_names={203999: "Jae Crowder"})

    assert "WOWY results (Version 1)" in output
    assert "player" in output
    assert "player_id" in output
    assert "Jae Crowder" in output
    assert "203999" in output
    assert "4.00" in output
