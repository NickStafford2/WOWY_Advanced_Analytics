"""WOWY metric package."""

from rawr_analytics.metrics.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy.dataset import (
    build_wowy_custom_query,
    prepare_wowy_player_season_records,
)
from rawr_analytics.metrics.wowy.defaults import (
    default_filters,
    describe_metric,
    describe_wowy_metric,
    describe_wowy_shrunk_metric,
)
from rawr_analytics.metrics.wowy.inputs import validate_filters
from rawr_analytics.metrics.wowy.models import (
    WowyCustomQueryResult,
    WowyCustomQueryRow,
    WowyGame,
    WowyPlayerContext,
    WowyPlayerSeasonRecord,
    WowyRequest,
    WowySeasonInput,
)
from rawr_analytics.metrics.wowy.records import build_player_season_records

__all__ = [
    "DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES",
    "WowyCustomQueryResult",
    "WowyCustomQueryRow",
    "WowyGame",
    "WowyPlayerContext",
    "WowyPlayerSeasonRecord",
    "WowyRequest",
    "WowySeasonInput",
    "build_player_season_records",
    "build_wowy_custom_query",
    "compute_wowy_shrinkage_score",
    "default_filters",
    "describe_metric",
    "describe_wowy_metric",
    "describe_wowy_shrunk_metric",
    "prepare_wowy_player_season_records",
    "validate_filters",
]
