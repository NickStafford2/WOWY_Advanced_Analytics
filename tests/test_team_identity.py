from __future__ import annotations

import pytest

from rawr_analytics.basketball.team_history import resolve_team_history_entry
from rawr_analytics.basketball.team_identity import (
    canonical_team_lookup_abbreviation,
    list_expected_team_abbreviations_for_season,
    resolve_team_id,
    resolve_team_identity,
    resolve_team_identity_from_id_and_date,
    team_is_active_for_season,
)


def test_list_expected_team_abbreviations_for_season_uses_exact_historical_codes() -> None:
    teams_1983 = list_expected_team_abbreviations_for_season("1983-84")

    assert "SDC" in teams_1983
    assert "KCK" in teams_1983
    assert "LAC" not in teams_1983
    assert "SAC" not in teams_1983


def test_resolve_team_identity_from_id_and_date_uses_exact_historical_label() -> None:
    clippers = resolve_team_identity_from_id_and_date(1610612746, "1984-03-10")
    kings = resolve_team_identity_from_id_and_date(1610612758, "1978-02-01")
    hornets = resolve_team_identity_from_id_and_date(1610612740, "2003-03-10")

    assert clippers.abbreviation == "SDC"
    assert kings.abbreviation == "KCK"
    assert hornets.abbreviation == "NOH"


def test_lookup_abbreviations_follow_cross_era_continuity() -> None:
    assert canonical_team_lookup_abbreviation("NJN") == "BKN"
    assert canonical_team_lookup_abbreviation("SEA") == "OKC"
    assert canonical_team_lookup_abbreviation("SDC") == "LAC"
    assert canonical_team_lookup_abbreviation("CHH") == "NOP"


def test_resolve_team_id_without_season_uses_continuity_lookup() -> None:
    assert resolve_team_id("NJN") == 1610612751
    assert resolve_team_id("BKN") == 1610612751
    assert resolve_team_id("SDC") == 1610612746
    assert resolve_team_id("LAC") == 1610612746


def test_resolve_team_identity_with_season_accepts_lookup_alias_and_returns_exact_label() -> None:
    assert resolve_team_identity("BKN", season="2008-09").abbreviation == "NJN"
    assert resolve_team_identity("NOP", season="2002-03").abbreviation == "NOH"


def test_team_activity_checks_reject_inactive_historical_scope() -> None:
    assert team_is_active_for_season("CHA", "2002-03") is False
    assert team_is_active_for_season("NOH", "2002-03") is True

    with pytest.raises(ValueError, match="was not active in season"):
        resolve_team_identity("CHA", season="2002-03")


def test_hornets_history_keeps_charlotte_and_new_orleans_split() -> None:
    charlotte_hornets = resolve_team_history_entry("CHH", season="2001-02")
    new_orleans_hornets = resolve_team_history_entry("NOH", season="2002-03")
    charlotte_expansion = resolve_team_history_entry("CHA", season="2004-05")

    assert charlotte_hornets.franchise_id == "hornets_original"
    assert new_orleans_hornets.franchise_id == "hornets_original"
    assert charlotte_expansion.franchise_id == "charlotte_expansion"
