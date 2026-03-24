from __future__ import annotations

import pytest

from wowy.nba.team_identity import (
    canonical_team_lookup_abbreviation,
    list_expected_team_abbreviations_for_season,
    resolve_team_history_entry,
    resolve_team_identity,
    team_is_active_for_season,
)


def test_list_expected_team_abbreviations_for_season_uses_explicit_historical_codes() -> None:
    teams_2002 = list_expected_team_abbreviations_for_season("2002-03")

    assert "NOH" in teams_2002
    assert "CHA" not in teams_2002
    assert "NOP" not in teams_2002


def test_hornets_history_is_explicitly_split_between_charlotte_and_new_orleans() -> None:
    charlotte_hornets = resolve_team_history_entry("CHH", season="2001-02")
    new_orleans_hornets = resolve_team_history_entry("NOH", season="2002-03")
    charlotte_expansion = resolve_team_history_entry("CHA", season="2004-05")

    assert charlotte_hornets.franchise_id == "hornets_original"
    assert new_orleans_hornets.franchise_id == "hornets_original"
    assert charlotte_expansion.franchise_id == "charlotte_expansion"


def test_team_activity_checks_reject_inactive_historical_scope() -> None:
    assert team_is_active_for_season("CHA", "2002-03") is False
    assert team_is_active_for_season("NOH", "2002-03") is True

    with pytest.raises(ValueError, match="was not active in season"):
        resolve_team_identity("CHA", season="2002-03")


def test_lookup_abbreviations_follow_explicit_franchise_lineage() -> None:
    assert canonical_team_lookup_abbreviation("NJN") == "BKN"
    assert canonical_team_lookup_abbreviation("SEA") == "OKC"
    assert canonical_team_lookup_abbreviation("CHH") == "NOP"
    assert canonical_team_lookup_abbreviation("NOH") == "NOP"
