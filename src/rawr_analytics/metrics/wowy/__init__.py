"""WOWY metric package.

The public workflow interfaces live in subpackages:
calculate turns normalized game records into WOWY player-season records, refresh
builds metric-store rows for precomputed snapshots, and query resolves user
filters into records, payloads, and exports.
"""

from rawr_analytics.metrics.wowy.cache import load_wowy_records
from rawr_analytics.metrics.wowy.calculate.inputs import (
    validate_filters,
)
from rawr_analytics.metrics.wowy.calculate.records import (
    WowyPlayerSeasonRecord,
    WowyPlayerSeasonValue,
    build_player_season_records,
    build_wowy_custom_query,
    prepare_wowy_player_season_records,
)
from rawr_analytics.metrics.wowy.calculate.shrinkage import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy.defaults import (
    default_filters,
    describe_metric,
    describe_wowy_metric,
    describe_wowy_shrunk_metric,
)
from rawr_analytics.metrics.wowy.refresh.records import (
    build_wowy_refresh_records,
)

__all__ = [
    "DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES",
    "WowyPlayerSeasonRecord",
    "WowyPlayerSeasonValue",
    "build_player_season_records",
    "build_wowy_custom_query",
    "build_wowy_refresh_records",
    "compute_wowy_shrinkage_score",
    "default_filters",
    "describe_metric",
    "describe_wowy_metric",
    "describe_wowy_shrunk_metric",
    "load_wowy_records",
    "prepare_wowy_player_season_records",
    "validate_filters",
]
