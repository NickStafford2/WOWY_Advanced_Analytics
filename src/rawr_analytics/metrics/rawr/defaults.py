from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.rawr._shrinkage import RawrShrinkageMode

DEFAULT_RAWR_SHRINKAGE_MODE = RawrShrinkageMode.UNIFORM
DEFAULT_RAWR_SHRINKAGE_STRENGTH = 1.0
DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE = 48.0


@dataclass(frozen=True)
class RawrDefaultFilters:
    min_average_minutes: float = 30.0
    min_total_minutes: float = 600.0
    top_n: int = 30
    min_games: int = 35
    ridge_alpha: float = 10.0


DEFAULT_RAWR_FILTERS = RawrDefaultFilters()


def describe_rawr_metric() -> MetricSummary:
    return MetricSummary(Metric.RAWR, "rawr-player-season-v3")
