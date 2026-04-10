from __future__ import annotations

from pathlib import Path

import pytest
import rawr_analytics.data.game_cache._repository as game_cache_repository
import rawr_analytics.data.game_cache.schema as game_cache_schema
from rawr_analytics.data.game_cache.rows import NormalizedGamePlayerRow, NormalizedGameRow

from rawr_analytics.data.game_cache import (
    list_cached_team_seasons,
    replace_team_season_normalized_rows,
)
from rawr_analytics.shared.player import PlayerSummary
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def _set_normalized_cache_db_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "app" / "normalized_cache.sqlite3"
    monkeypatch.setattr(game_cache_schema, "NORMALIZED_CACHE_DB_PATH", db_path)
    monkeypatch.setattr(game_cache_repository, "NORMALIZED_CACHE_DB_PATH", db_path)


def test_list_cached_team_seasons_returns_unique_sorted_team_seasons(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_normalized_cache_db_path(tmp_path, monkeypatch)

    regular_season_2014 = Season.parse("2014", "Regular Season")
    regular_season_2015 = Season.parse("2015", "Regular Season")
    celtics = Team.from_abbreviation("BOS")
    hawks = Team.from_abbreviation("ATL")
    lakers = Team.from_abbreviation("LAL")

    replace_team_season_normalized_rows(
        team=celtics,
        season=regular_season_2014,
        games=[
            NormalizedGameRow(
                game_id="1",
                season=regular_season_2014,
                game_date="2015-04-01",
                team=celtics,
                opponent_team=hawks,
                is_home=True,
                margin=5.0,
                source="test",
            )
        ],
        game_players=[
            NormalizedGamePlayerRow(
                game_id="1",
                player=PlayerSummary(player_id=101, player_name="Player 101"),
                appeared=True,
                minutes=34.0,
                team=celtics,
            )
        ],
        source_path="tests/fixtures/bos_2014.csv",
        source_snapshot="snapshot-bos-2014",
        source_kind="test",
    )
    replace_team_season_normalized_rows(
        team=lakers,
        season=regular_season_2015,
        games=[
            NormalizedGameRow(
                game_id="2",
                season=regular_season_2015,
                game_date="2016-04-01",
                team=lakers,
                opponent_team=celtics,
                is_home=True,
                margin=2.0,
                source="test",
            )
        ],
        game_players=[
            NormalizedGamePlayerRow(
                game_id="2",
                player=PlayerSummary(player_id=24, player_name="Player 24"),
                appeared=True,
                minutes=36.0,
                team=lakers,
            )
        ],
        source_path="tests/fixtures/lal_2015.csv",
        source_snapshot="snapshot-lal-2015",
        source_kind="test",
    )

    assert list_cached_team_seasons() == [
        TeamSeasonScope(team=celtics, season=regular_season_2014),
        TeamSeasonScope(team=lakers, season=regular_season_2015),
    ]
