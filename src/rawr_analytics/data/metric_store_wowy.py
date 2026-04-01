from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.metric_store_query import require_current_metric_scope
from rawr_analytics.data.metric_store import (
    WowyPlayerSeasonValueRow,
    load_wowy_player_season_value_rows,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class WowyCachedPlayerSeasonsSnapshot:
    rows: list[WowyPlayerSeasonValueRow]


@dataclass(frozen=True)
class WowyCachedLeaderboardSnapshot:
    available_seasons: list[Season]
    available_teams: list[Team]
    rows: list[WowyPlayerSeasonValueRow]
    season_ids: list[str]


def load_wowy_cached_player_seasons_snapshot(
    *,
    metric: Metric,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_games_with: int | None,
    min_games_without: int | None,
) -> WowyCachedPlayerSeasonsSnapshot:
    require_current_metric_scope(metric=metric, scope_key=scope_key)
    rows = load_wowy_player_season_value_rows(
        metric_id=metric.value,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
    )
    return WowyCachedPlayerSeasonsSnapshot(rows=rows)


def load_wowy_cached_leaderboard_snapshot(
    *,
    metric: Metric,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_games_with: int | None,
    min_games_without: int | None,
) -> WowyCachedLeaderboardSnapshot:
    catalog_row = require_current_metric_scope(metric=metric, scope_key=scope_key)
    rows = load_wowy_player_season_value_rows(
        metric_id=metric.value,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
    )
    resolved_season_type = SeasonType.parse(catalog_row.season_type)
    return WowyCachedLeaderboardSnapshot(
        available_seasons=[
            Season(season_id, resolved_season_type.to_nba_format())
            for season_id in catalog_row.available_season_ids
        ],
        available_teams=[Team.from_id(team_id) for team_id in catalog_row.available_team_ids],
        rows=rows,
        season_ids=seasons or catalog_row.available_season_ids,
    )
