from __future__ import annotations

from wowy.metrics.wowy.formatting import format_results_table
from wowy.metrics.wowy.models import WowyPlayerStats


def test_format_results_table_contains_expected_text():
    results: dict[int, WowyPlayerStats] = {203999: WowyPlayerStats(3, 2, 5.0, 1.0, 4.0, 28.5, 57.0)}

    output = format_results_table(results, player_names={203999: "Jae Crowder"})

    assert "WOWY results (Version 1)" in output
    assert "player" in output
    assert "player_id" in output
    assert "Jae Crowder" in output
    assert "203999" in output
    assert "28.5" in output
    assert "57.0" in output
    assert "4.00" in output
