"""RAWR metric package.

The public workflow interfaces live in subpackages:
calculate turns normalized game records into RAWR player-season records, refresh
builds metric-store rows for precomputed snapshots, and query resolves user
filters into records, payloads, and exports.
"""

from rawr_analytics.metrics.rawr.calculate.inputs import validate_filters
from rawr_analytics.metrics.rawr.calculate.records import (
    RawrPlayerSeasonRecord,
    build_player_season_records,
)
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    DEFAULT_RAWR_TOP_N,
    RAWR_METRIC_SUMMARY,
)
from rawr_analytics.metrics.rawr.refresh.records import (
    RawrSeasonProgressFn,
    build_rawr_refresh_records,
    list_incomplete_rawr_season_warnings,
    load_rawr_records,
)

__all__ = [
    "DEFAULT_RAWR_RIDGE_ALPHA",
    "DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE",
    "DEFAULT_RAWR_SHRINKAGE_MODE",
    "DEFAULT_RAWR_SHRINKAGE_STRENGTH",
    "DEFAULT_RAWR_TOP_N",
    "RAWR_METRIC_SUMMARY",
    "RawrPlayerSeasonRecord",
    "RawrSeasonProgressFn",
    "RawrShrinkageMode",
    "build_player_season_records",
    "build_rawr_refresh_records",
    "list_incomplete_rawr_season_warnings",
    "load_rawr_records",
    "validate_filters",
]
