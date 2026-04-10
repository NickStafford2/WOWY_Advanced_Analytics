from __future__ import annotations

from rawr_analytics.data.game_cache.store import store_team_season_cache
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.sources.nba_api.ingest._models import IngestResult


def store_team_season(result: IngestResult) -> None:
    store_team_season_cache(
        scope=TeamSeasonScope(team=result.request.team, season=result.request.season),
        games=result.games,
        game_players=result.game_players,
        source_path=(
            "nba_api://normalized_games/"
            f"{result.request.team.abbreviation(season=result.request.season)}_"
            f"{result.request.season.year_string_nba_api}_{result.request.season.season_type.to_slug()}"
        ),
        source_snapshot="ingest-build-v2:nba_api",
        source_kind="nba_api",
        expected_games_count=result.summary.total_games,
        skipped_games_count=0,
    )


__all__ = [
    "store_team_season",
]
