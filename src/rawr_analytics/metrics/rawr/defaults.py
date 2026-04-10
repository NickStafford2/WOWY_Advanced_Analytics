from __future__ import annotations

from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode

RAWR_METRIC_SUMMARY = MetricSummary(Metric.RAWR, "rawr-player-season-v4")
DEFAULT_RAWR_TOP_N = 30
DEFAULT_RAWR_MIN_AVERAGE_MINUTES = 30.0
DEFAULT_RAWR_MIN_TOTAL_MINUTES = 600.0
DEFAULT_RAWR_MIN_GAMES = 35
DEFAULT_RAWR_RIDGE_ALPHA = 10.0
DEFAULT_RAWR_SHRINKAGE_MODE = RawrShrinkageMode.UNIFORM
DEFAULT_RAWR_SHRINKAGE_STRENGTH = 1.0
DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE = 48.0
