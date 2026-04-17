"""RAWR metric package.

The public workflow interfaces live in subpackages:
calculate turns normalized game records into RAWR player-season records, and
query resolves user filters into records, payloads, exports, and retained cache
writes.
"""

from rawr_analytics.metrics.rawr.cache import load_rawr_records
from rawr_analytics.metrics.rawr.cache_status import list_incomplete_rawr_season_warnings
from rawr_analytics.metrics.rawr.calculate.inputs import validate_filters
from rawr_analytics.metrics.rawr.calculate.records import (
    RawrPlayerSeasonRecord,
    build_player_season_records,
)
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode

__all__ = [
    "RawrPlayerSeasonRecord",
    "RawrShrinkageMode",
    "build_player_season_records",
    "list_incomplete_rawr_season_warnings",
    "load_rawr_records",
    "validate_filters",
]
