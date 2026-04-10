from __future__ import annotations

from rawr_analytics.metrics.constants import Metric, MetricSummary


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
        build_version="wowy-player-season-v4",
    )


def describe_wowy_shrunk_metric() -> MetricSummary:
    return MetricSummary(
        metric=Metric.WOWY_SHRUNK,
        build_version="wowy-shrunk-player-season-v2",
    )


def describe_metric(metric: Metric) -> MetricSummary:
    if metric == Metric.WOWY:
        return describe_wowy_metric()
    if metric == Metric.WOWY_SHRUNK:
        return describe_wowy_shrunk_metric()
    raise ValueError(f"Unknown WOWY metric: {metric}")
