from __future__ import annotations

from pathlib import Path

import pytest

from wowy.data.normalized_io import load_normalized_games_from_csv
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.team_seasons import parse_team_season_filename


def test_canonicalize_season_string_accepts_single_year_input():
    assert canonicalize_season_string("2014") == "2014-15"
    assert canonicalize_season_string("2014-15") == "2014-15"


def test_canonicalize_season_string_rejects_noncanonical_hyphenated_value():
    with pytest.raises(ValueError, match="Expected canonical season"):
        canonicalize_season_string("2014-14")


def test_parse_team_season_filename_rejects_noncanonical_season_key():
    with pytest.raises(ValueError, match="Non-canonical season key"):
        parse_team_season_filename(Path("BOS_2014.csv"))


def test_load_normalized_games_from_csv_rejects_noncanonical_season_value(
    tmp_path: Path,
):
    csv_path = tmp_path / "games.csv"
    csv_path.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2014,2015-04-01,BOS,ATL,true,5,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="expected canonical season '2014-15'"):
        load_normalized_games_from_csv(csv_path)
