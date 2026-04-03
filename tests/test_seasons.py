from __future__ import annotations

from pathlib import Path

import pytest

from rawr_analytics.data.game_cache import list_cached_team_seasons
from rawr_analytics.data.scopes import TeamSeasonScope
from rawr_analytics.basketball.seasons import canonicalize_season_year_string
from tests.support import game, player, seed_db_from_team_seasons


def test_canonicalize_season_string_accepts_single_year_input():
    assert canonicalize_season_year_string("2014") == "2014-15"
    assert canonicalize_season_year_string("2014-15") == "2014-15"


def test_canonicalize_season_string_rejects_noncanonical_hyphenated_value():
    with pytest.raises(ValueError, match="Expected canonical season"):
        canonicalize_season_year_string("2014-14")


def test_list_cached_team_seasons_returns_db_rows(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(
        db_path,
        [
            (
                "BOS",
                "2014-15",
                [game("1", "2014-15", "2015-04-01", "BOS", "ATL", True, 5.0)],
                [player("1", "BOS", 101, "Player 101", True, 34.0)],
            )
        ],
    )

    assert list_cached_team_seasons(
        player_metrics_db_path=db_path,
        season_type="Regular Season",
    ) == [TeamSeasonScope(team="BOS", season="2014-15", team_id=1610612738)]


def test_list_cached_team_seasons_returns_multiple_db_rows(
    tmp_path: Path,
):
    db_path = tmp_path / "app" / "player_metrics.sqlite3"
    seed_db_from_team_seasons(
        db_path,
        [
            (
                "BOS",
                "2014-15",
                [game("1", "2014-15", "2015-04-01", "BOS", "ATL", True, 5.0)],
                [player("1", "BOS", 101, "Player 101", True, 34.0)],
            ),
            (
                "LAL",
                "2015-16",
                [game("2", "2015-16", "2016-04-01", "LAL", "BOS", True, 2.0)],
                [player("2", "LAL", 24, "Player 24", True, 36.0)],
            ),
        ],
    )

    assert list_cached_team_seasons(
        player_metrics_db_path=db_path,
        season_type="Regular Season",
    ) == [
        TeamSeasonScope(team="BOS", season="2014-15", team_id=1610612738),
        TeamSeasonScope(team="LAL", season="2015-16", team_id=1610612747),
    ]
