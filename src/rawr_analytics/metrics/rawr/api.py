from __future__ import annotations

from pathlib import Path
from typing import Any

from rawr_analytics.data.player_metrics_db.constants import DEFAULT_PLAYER_METRICS_DB_PATH
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
    "build_custom_query_rows",
    "validate_filters",
]


def build_cached_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    db_path: Path,
    teams: list[str] | None,
    team_ids: list[int] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    return build_rawr_metric_rows(
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=season_type,
        db_path=db_path,
        teams=teams,
        team_ids=team_ids,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )


def build_custom_query_rows(
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: str,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
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
        player_metrics_db_path=player_metrics_db_path,
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
