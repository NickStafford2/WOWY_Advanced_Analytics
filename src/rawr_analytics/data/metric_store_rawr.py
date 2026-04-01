from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.metric_store_query import require_current_metric_scope
from rawr_analytics.data.player_metrics_db import (
    RawrPlayerSeasonValueRow,
    load_rawr_player_season_value_rows,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrCachedPlayerSeasonsSnapshot:
    rows: list[RawrPlayerSeasonValueRow]


@dataclass(frozen=True)
class RawrCachedLeaderboardSnapshot:
    available_seasons: list[Season]
    available_teams: list[Team]
    rows: list[RawrPlayerSeasonValueRow]
    season_ids: list[str]


def load_rawr_cached_player_seasons_snapshot(
    *,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_games: int | None,
) -> RawrCachedPlayerSeasonsSnapshot:
    require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
    rows = load_rawr_player_season_value_rows(
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_games=min_games,
    )
    return RawrCachedPlayerSeasonsSnapshot(rows=rows)


def load_rawr_cached_leaderboard_snapshot(
    *,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_games: int | None,
) -> RawrCachedLeaderboardSnapshot:
    catalog_row = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
    rows = load_rawr_player_season_value_rows(
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_games=min_games,
    )
    resolved_season_type = SeasonType.parse(catalog_row.season_type)
    return RawrCachedLeaderboardSnapshot(
        available_seasons=[
            Season(season_id, resolved_season_type.to_nba_format())
            for season_id in catalog_row.available_season_ids
        ],
        available_teams=[Team.from_id(team_id) for team_id in catalog_row.available_team_ids],
        rows=rows,
        season_ids=seasons or catalog_row.available_season_ids,
    )
