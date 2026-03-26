from __future__ import annotations

from typing import Any

from rawr_analytics.data.player_metrics_db.models import PlayerSeasonMetricRow
from rawr_analytics.metrics.rawr.data import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    RAWR_METRIC,
    build_rawr_metric_rows,
)
from rawr_analytics.metrics.rawr.records import prepare_rawr_player_season_records
from rawr_analytics.metrics.rawr.service import validate_filters

__all__ = [
    "RAWR_METRIC",
    "build_cached_rows",
    "build_custom_query",
    "build_custom_query_rows",
    "default_filters",
    "describe_metric",
    "validate_filters",
]


def default_filters() -> dict[str, int | float]:
    return {
        "min_average_minutes": 30.0,
        "min_total_minutes": 600.0,
        "top_n": 30,
        "min_games": 35,
        "ridge_alpha": 10.0,
    }


def describe_metric(metric: str) -> dict[str, str]:
    if metric != RAWR_METRIC:
        raise ValueError(f"Unknown RAWR metric: {metric}")
    return {
        "metric": RAWR_METRIC,
        "label": "RAWR",
        "build_version": "rawr-player-season-v3",
    }


def build_cached_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    teams: list[str] | None,
    team_ids: list[int] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    return build_rawr_metric_rows(
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=season_type,
        teams=teams,
        team_ids=team_ids,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )


def build_custom_query(
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: str,
    min_games: int,
    ridge_alpha: float,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    return {
        "metric": RAWR_METRIC,
        "metric_label": describe_metric(RAWR_METRIC)["label"],
        "rows": build_custom_query_rows(
            teams=teams,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            min_games=min_games,
            ridge_alpha=ridge_alpha,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        ),
    }


def build_custom_query_rows(
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: str,
    min_games: int,
    ridge_alpha: float,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> list[dict[str, Any]]:
    records = prepare_rawr_player_season_records(
        teams=teams,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return [
        {
            "season": record.season,
            "player_id": record.player_id,
            "player_name": record.player_name,
            "value": record.coefficient,
            "sample_size": record.games,
            "secondary_sample_size": None,
            "games": record.games,
            "average_minutes": record.average_minutes,
            "total_minutes": record.total_minutes,
        }
        for record in records
    ]
