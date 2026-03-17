from __future__ import annotations

from pathlib import Path

import pytest

from wowy.data.game_cache_db import replace_team_season_normalized_rows
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.team_seasons import TeamSeasonScope, list_cached_team_seasons


def test_canonicalize_season_string_accepts_single_year_input():
    assert canonicalize_season_string("2014") == "2014-15"
    assert canonicalize_season_string("2014-15") == "2014-15"


def test_canonicalize_season_string_rejects_noncanonical_hyphenated_value():
    with pytest.raises(ValueError, match="Expected canonical season"):
        canonicalize_season_string("2014-14")


def test_list_cached_team_seasons_returns_db_rows(
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
        player_metrics_db_path=db_path,
        season_type="Regular Season",
    ) == [TeamSeasonScope(team="BOS", season="2014-15")]


def test_list_cached_team_seasons_returns_multiple_db_rows(
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
    replace_team_season_normalized_rows(
        db_path,
        team="LAL",
        season="2015-16",
        season_type="Regular Season",
        games=[
            NormalizedGameRecord(
                game_id="2",
                season="2015-16",
                game_date="2016-04-01",
                team="LAL",
                opponent="BOS",
                is_home=True,
                margin=2.0,
                season_type="Regular Season",
                source="nba_api",
            )
        ],
        game_players=[
            NormalizedGamePlayerRecord(
                game_id="2",
                team="LAL",
                player_id=24,
                player_name="Player 24",
                appeared=True,
                minutes=36.0,
            )
        ],
        source_path="db-only",
        source_snapshot="db-only",
        source_kind="test",
    )

    assert list_cached_team_seasons(
        player_metrics_db_path=db_path,
        season_type="Regular Season",
    ) == [
        TeamSeasonScope(team="BOS", season="2014-15"),
        TeamSeasonScope(team="LAL", season="2015-16"),
    ]
