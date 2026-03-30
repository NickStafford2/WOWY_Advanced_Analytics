from __future__ import annotations

from typing import Any

from rawr_analytics.data.player_metrics_db.models import PlayerSeasonMetricRow
from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy.records import (
    build_wowy_metric_rows,
    build_wowy_shrunk_metric_rows,
    prepare_wowy_player_season_records,
)
from rawr_analytics.metrics.wowy.service import validate_filters
from rawr_analytics.shared.season import SeasonType

__all__ = [
    "default_filters",
    "describe_metric",
    "describe_wowy_shrunk_metric",
    "describe_wowy_metric",
    "validate_filters",
]


def default_filters() -> dict[str, int | float]:
    return {
        "min_average_minutes": 30.0,
        "min_total_minutes": 600.0,
        "top_n": 30,
        "min_games_with": 15,
        "min_games_without": 2,
    }


def describe_wowy_metric() -> MetricSummary:
    return MetricSummary(
        metric=Metric.WOWY,
        label="WOWY",
        build_version="wowy-player-season-v3",
    )


def describe_wowy_shrunk_metric() -> MetricSummary:
    return MetricSummary(
        metric=Metric.WOWY_SHRUNK,
        label="WOWY Shrunk",
        build_version="wowy-shrunk-player-season-v1",
    )


def describe_metric(metric: Metric) -> MetricSummary:
    if metric == Metric.WOWY:
        return describe_wowy_metric()
    if metric == Metric.WOWY_SHRUNK:
        return describe_wowy_shrunk_metric()
    raise ValueError(f"Unknown WOWY metric: {metric}")


def build_cached_rows(
    metric: Metric,
    *,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
    teams: list[str] | None,
    team_ids: list[int] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    if metric == Metric.WOWY:
        return build_wowy_metric_rows(
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            teams=teams,
            team_ids=team_ids,
            rawr_ridge_alpha=rawr_ridge_alpha,
        )
    if metric == Metric.WOWY_SHRUNK:
        return build_wowy_shrunk_metric_rows(
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            teams=teams,
            team_ids=team_ids,
            rawr_ridge_alpha=rawr_ridge_alpha,
        )
    raise ValueError(f"Unknown WOWY metric: {metric}")


def build_custom_query(
    metric: Metric,
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: SeasonType,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    return {
        "metric": metric.value,
        "metric_label": describe_metric(metric).label,
        "rows": build_custom_query_rows(
            metric,
            teams=teams,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            min_games_with=min_games_with,
            min_games_without=min_games_without,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        ),
    }


def build_custom_query_rows(
    metric: Metric,
    *,
    teams: list[str] | None,
    team_ids: list[int] | None,
    seasons: list[str] | None,
    season_type: SeasonType,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> list[dict[str, Any]]:
    records = prepare_wowy_player_season_records(
        teams=teams,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )

    rows: list[dict[str, Any]] = []
    for record in records:
        row = {
            "season": record.season,
            "player_id": record.player_id,
            "player_name": record.player_name,
            "sample_size": record.games_with,
            "secondary_sample_size": record.games_without,
            "games_with": record.games_with,
            "games_without": record.games_without,
            "avg_margin_with": record.avg_margin_with,
            "avg_margin_without": record.avg_margin_without,
            "average_minutes": record.average_minutes,
            "total_minutes": record.total_minutes,
        }
        if metric == Metric.WOWY:
            row["value"] = record.wowy_score
        elif metric == Metric.WOWY_SHRUNK:
            row["value"] = compute_wowy_shrinkage_score(
                games_with=record.games_with,
                games_without=record.games_without,
                wowy_score=record.wowy_score,
                prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
            )
            row["raw_wowy_score"] = record.wowy_score
        else:
            raise ValueError(f"Unknown WOWY metric: {metric}")
        rows.append(row)
    return rows
