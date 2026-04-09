from __future__ import annotations

from rawr_analytics.shared.team import Team


def test_all_active_in_season_uses_exact_historical_abbreviations() -> None:
    abbreviations = {team.abbreviation(season=1983) for team in Team.all_active_in_season(1983)}

    assert "SDC" in abbreviations
    assert "KCK" in abbreviations
    assert "LAC" not in abbreviations
    assert "SAC" not in abbreviations


def test_team_lookup_returns_exact_historical_team_season() -> None:
    clippers = Team.from_abbreviation("SDC", season=1983).for_season(1983)
    kings = Team.from_abbreviation("KCK", season=1978).for_season(1978)
    hornets = Team.from_abbreviation("NOH", game_date="2003-03-10").for_date("2003-03-10")

    assert clippers.full_name == "San Diego Clippers"
    assert clippers.abbreviation == "SDC"
    assert kings.full_name == "Kansas City Kings"
    assert kings.abbreviation == "KCK"
    assert hornets.full_name == "New Orleans Hornets"
    assert hornets.abbreviation == "NOH"


def test_historical_records_keep_the_same_team_id_as_current_source_data() -> None:
    assert Team.from_abbreviation("SDC", season=1983).team_id == Team.from_abbreviation("LAC").team_id
    assert Team.from_abbreviation("KCK", season=1978).team_id == Team.from_abbreviation("SAC").team_id
    assert Team.from_abbreviation("NOH", season=2002).team_id == Team.from_abbreviation("NOP").team_id
