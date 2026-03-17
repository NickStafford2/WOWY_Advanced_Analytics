from __future__ import annotations

from pathlib import Path

import pytest

from wowy.data.game_cache_db import replace_team_season_normalized_rows
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.data.normalized_io import load_normalized_games_from_csv
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.team_seasons import list_cached_team_seasons, parse_team_season_filename


def test_canonicalize_season_string_accepts_single_year_input():
    assert canonicalize_season_string("2014") == "2014-15"
    assert canonicalize_season_string("2014-15") == "2014-15"


def test_canonicalize_season_string_rejects_noncanonical_hyphenated_value():
    with pytest.raises(ValueError, match="Expected canonical season"):
        canonicalize_season_string("2014-14")


def test_parse_team_season_filename_rejects_noncanonical_season_key():
    with pytest.raises(ValueError, match="Non-canonical season key"):
        parse_team_season_filename(Path("BOS_2014.csv"))


def test_parse_team_season_filename_accepts_season_type_suffix():
    assert parse_team_season_filename(Path("BOS_2014-15_playoffs.csv")) == (
        parse_team_season_filename(Path("BOS_2014-15.csv"))
    )


def test_list_cached_team_seasons_deduplicates_regular_and_playoff_files(
    tmp_path: Path,
):
    (tmp_path / "BOS_2014-15.csv").write_text("", encoding="utf-8")
    (tmp_path / "BOS_2014-15_playoffs.csv").write_text("", encoding="utf-8")

    assert list_cached_team_seasons(tmp_path) == [
        parse_team_season_filename(Path("BOS_2014-15.csv"))
    ]


def test_list_cached_team_seasons_falls_back_to_db_when_csv_cache_is_missing(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    replace_team_season_normalized_rows(
        db_path,
        team="BOS",
        season="2014-15",
        season_type="Regular Season",
        games=[
            NormalizedGameRecord(
                game_id="1",
                season="2014-15",
                game_date="2015-04-01",
                team="BOS",
                opponent="ATL",
                is_home=True,
                margin=5.0,
                season_type="Regular Season",
                source="nba_api",
            )
        ],
        game_players=[
            NormalizedGamePlayerRecord(
                game_id="1",
                team="BOS",
                player_id=101,
                player_name="Player 101",
                appeared=True,
                minutes=34.0,
            )
        ],
        source_path="db-only",
        source_snapshot="db-only",
        source_kind="test",
    )

    assert list_cached_team_seasons(
        tmp_path / "missing-normalized-games",
        player_metrics_db_path=db_path,
        season_type="Regular Season",
    ) == [parse_team_season_filename(Path("BOS_2014-15.csv"))]


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
