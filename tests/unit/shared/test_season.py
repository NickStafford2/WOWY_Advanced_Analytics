from __future__ import annotations

import pytest

from rawr_analytics.shared.season import Season


def test_season_parse_accepts_single_year_and_canonical_span() -> None:
    assert Season.parse("2014", "Regular Season").id == "2014-15:REGULAR"
    assert Season.parse("2014-15", "Regular Season").id == "2014-15:REGULAR"


def test_season_parse_rejects_noncanonical_span() -> None:
    with pytest.raises(AssertionError, match="Expected 2014-15"):
        Season.parse("2014-14", "Regular Season")
