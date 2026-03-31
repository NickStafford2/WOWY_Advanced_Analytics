from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rawr_analytics.data.metric_store_query import require_current_metric_scope
from rawr_analytics.data.player_metrics_db.models import PlayerSeasonMetricRow
from rawr_analytics.data.player_metrics_db.queries import (
    load_metric_full_span_points_map,
    load_metric_full_span_series_rows,
    load_metric_rows,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

__all__ = [
    "CachedMetricLeaderboardSnapshot",
    "CachedMetricPlayerSeasonsSnapshot",
    "CachedMetricSpanSnapshot",
    "load_cached_metric_leaderboard_snapshot",
    "load_cached_metric_player_seasons_snapshot",
    "load_cached_metric_span_snapshot",
]


@dataclass(frozen=True)
class CachedMetricPlayerSeasonsSnapshot:
    metric: str
    metric_label: str
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class CachedMetricLeaderboardSnapshot:
    metric: str
    metric_label: str
    available_seasons: list[Season]
    available_teams: list[Team]
    rows: list[dict[str, Any]]
    season_ids: list[str]


@dataclass(frozen=True)
class CachedMetricSpanSnapshot:
    metric: str
    metric_label: str
    start_season: str | None
    end_season: str | None
    available_seasons: list[str]
    top_n: int
    series: list[dict[str, Any]]


def load_cached_metric_player_seasons_snapshot(
    *,
    metric: Metric,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_sample_size: int | None,
    min_secondary_sample_size: int | None,
) -> CachedMetricPlayerSeasonsSnapshot:
    catalog_row = require_current_metric_scope(metric=metric, scope_key=scope_key)
    rows = load_metric_rows(
        metric=metric.value,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_sample_size=min_sample_size,
        min_secondary_sample_size=min_secondary_sample_size,
    )
    return CachedMetricPlayerSeasonsSnapshot(
        metric=metric.value,
        metric_label=catalog_row.label,
        rows=[_serialize_metric_player_season_row(row) for row in rows],
    )


def load_cached_metric_leaderboard_snapshot(
    *,
    metric: Metric,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_sample_size: int | None,
    min_secondary_sample_size: int | None,
) -> CachedMetricLeaderboardSnapshot:
    catalog_row = require_current_metric_scope(metric=metric, scope_key=scope_key)
    rows = load_metric_rows(
        metric=metric.value,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_sample_size=min_sample_size,
        min_secondary_sample_size=min_secondary_sample_size,
    )
    resolved_season_type = SeasonType.parse(catalog_row.season_type)
    return CachedMetricLeaderboardSnapshot(
        metric=metric.value,
        metric_label=catalog_row.label,
        available_seasons=[
            Season(season_id, resolved_season_type.to_nba_format())
            for season_id in catalog_row.available_season_ids
        ],
        available_teams=[Team.from_id(team_id) for team_id in catalog_row.available_team_ids],
        rows=[_serialize_metric_player_season_row(row) for row in rows],
        season_ids=seasons or catalog_row.available_season_ids,
    )


def load_cached_metric_span_snapshot(
    *,
    metric: Metric,
    scope_key: str,
    top_n: int,
) -> CachedMetricSpanSnapshot:
    catalog_row = require_current_metric_scope(metric=metric, scope_key=scope_key)
    series_rows = load_metric_full_span_series_rows(
        metric=metric.value,
        scope_key=scope_key,
        top_n=top_n,
    )
    player_ids = [row.player_id for row in series_rows]
    season_points = load_metric_full_span_points_map(
        metric=metric.value,
        scope_key=scope_key,
        player_ids=player_ids,
    )
    return CachedMetricSpanSnapshot(
        metric=metric.value,
        metric_label=catalog_row.label,
        start_season=catalog_row.full_span_start_season_id,
        end_season=catalog_row.full_span_end_season_id,
        available_seasons=catalog_row.available_season_ids,
        top_n=top_n,
        series=[
            {
                "player_id": row.player_id,
                "player_name": row.player_name,
                "span_average_value": row.span_average_value,
                "season_count": row.season_count,
                "points": [
                    {
                        "season": season,
                        "value": season_points.get(row.player_id, {}).get(season),
                    }
                    for season in catalog_row.available_season_ids
                ],
            }
            for row in series_rows
        ],
    )


def _serialize_metric_player_season_row(row: PlayerSeasonMetricRow) -> dict[str, Any]:
    payload = {
        "season": row.season_id,
        "player_id": row.player_id,
        "player_name": row.player_name,
        "value": row.value,
        "sample_size": row.sample_size,
        "secondary_sample_size": row.secondary_sample_size,
        "average_minutes": row.average_minutes,
        "total_minutes": row.total_minutes,
    }
    payload.update(row.details or {})
    return payload
