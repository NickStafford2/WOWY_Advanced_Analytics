"""RAWR metric package."""

from rawr_analytics.metrics.rawr._shrinkage import RawrShrinkageMode
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_MIN_AVERAGE_MINUTES,
    DEFAULT_RAWR_MIN_GAMES,
    DEFAULT_RAWR_MIN_TOTAL_MINUTES,
    DEFAULT_RAWR_RIDGE_ALPHA,
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    DEFAULT_RAWR_TOP_N,
    RAWR_METRIC_SUMMARY,
)
from rawr_analytics.metrics.rawr.inputs import (
    RawrRequestDTO,
    RawrSeasonInputDTO,
    build_rawr_request,
    validate_filters,
    validate_request,
)
from rawr_analytics.metrics.rawr.records import RawrPlayerSeasonRecord, build_player_season_records

__all__ = [
    "DEFAULT_RAWR_MIN_AVERAGE_MINUTES",
    "DEFAULT_RAWR_MIN_GAMES",
    "DEFAULT_RAWR_MIN_TOTAL_MINUTES",
    "DEFAULT_RAWR_RIDGE_ALPHA",
    "DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE",
    "DEFAULT_RAWR_SHRINKAGE_MODE",
    "DEFAULT_RAWR_SHRINKAGE_STRENGTH",
    "DEFAULT_RAWR_TOP_N",
    "RAWR_METRIC_SUMMARY",
    "RawrPlayerSeasonRecord",
    "RawrRequestDTO",
    "RawrSeasonInputDTO",
    "RawrShrinkageMode",
    "build_player_season_records",
    "build_rawr_request",
    "validate_filters",
    "validate_request",
]
