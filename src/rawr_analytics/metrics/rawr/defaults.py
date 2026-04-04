from __future__ import annotations

from rawr_analytics.metrics.constants import Metric, MetricSummary

DEFAULT_RAWR_SHRINKAGE_MODE = "uniform"
DEFAULT_RAWR_SHRINKAGE_STRENGTH = 1.0
DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE = 48.0


def default_filters() -> dict[str, int | float]:
    return {
        "min_average_minutes": 30.0,
        "min_total_minutes": 600.0,
        "top_n": 30,
        "min_games": 35,
        "ridge_alpha": 10.0,
    }


def describe_rawr_metric() -> MetricSummary:
    return MetricSummary(Metric.RAWR, "RAWR", "rawr-player-season-v3")
