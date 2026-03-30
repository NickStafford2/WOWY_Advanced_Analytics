from __future__ import annotations

from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.wowy._analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy._inputs import _validate_filters, _validate_request
from rawr_analytics.metrics.wowy._records import _build_player_season_records
from rawr_analytics.metrics.wowy.models import WowyPlayerSeasonRecord, WowyRequest

__all__ = [
    "DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES",
    "build_player_season_records",
    "compute_wowy_shrinkage_score",
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


def validate_filters(
    min_games_with: int,
    min_games_without: int,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> None:
    _validate_filters(
        min_games_with,
        min_games_without,
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


def build_player_season_records(request: WowyRequest) -> list[WowyPlayerSeasonRecord]:
    _validate_request(request)
    return _build_player_season_records(request)
