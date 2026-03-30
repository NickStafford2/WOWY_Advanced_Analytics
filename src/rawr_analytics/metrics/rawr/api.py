from __future__ import annotations

from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.rawr._inputs import _validate_filters, _validate_request
from rawr_analytics.metrics.rawr._records import _build_player_season_records
from rawr_analytics.metrics.rawr.models import RawrPlayerSeasonRecord, RawrRequest

DEFAULT_RAWR_SHRINKAGE_MODE = "uniform"
DEFAULT_RAWR_SHRINKAGE_STRENGTH = 1.0
DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE = 48.0

__all__ = [
    "DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE",
    "DEFAULT_RAWR_SHRINKAGE_MODE",
    "DEFAULT_RAWR_SHRINKAGE_STRENGTH",
    "build_player_season_records",
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


def describe_metric() -> MetricSummary:
    return MetricSummary(Metric.RAWR, "RAWR", "rawr-player-season-v3")


def validate_filters(
    min_games: int,
    ridge_alpha: float,
    shrinkage_mode: str = DEFAULT_RAWR_SHRINKAGE_MODE,
    shrinkage_strength: float = DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    shrinkage_minute_scale: float = DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> None:
    _validate_filters(
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


def build_player_season_records(request: RawrRequest) -> list[RawrPlayerSeasonRecord]:
    _validate_request(request)
    return _build_player_season_records(request)
