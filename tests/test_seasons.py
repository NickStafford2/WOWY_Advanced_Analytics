from __future__ import annotations

from pathlib import Path

import pytest

from wowy.nba.models import CanonicalGamePlayerRecord, CanonicalGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.team_seasons import TeamSeasonScope, list_cached_team_seasons
from tests.support import seed_db_from_team_seasons


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
    seed_db_from_team_seasons(
        db_path,
        [
            (
                "BOS",
                "2014-15",
                [
                    CanonicalGameRecord(
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
                [
                    CanonicalGamePlayerRecord(
                        game_id="1",
                        team="BOS",
                        player_id=101,
                        player_name="Player 101",
                        appeared=True,
                        minutes=34.0,
                    )
                ],
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
                [
                    CanonicalGameRecord(
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
                [
                    CanonicalGamePlayerRecord(
                        game_id="1",
                        team="BOS",
                        player_id=101,
                        player_name="Player 101",
                        appeared=True,
                        minutes=34.0,
                    )
                ],
            ),
            (
                "LAL",
                "2015-16",
                [
                    CanonicalGameRecord(
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
                [
                    CanonicalGamePlayerRecord(
                        game_id="2",
                        team="LAL",
                        player_id=24,
                        player_name="Player 24",
                        appeared=True,
                        minutes=36.0,
                    )
                ],
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
