"""WOWY metric package."""

from rawr_analytics.metrics.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    WowyGame,
    WowyPlayerValue,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy.defaults import (
    default_filters,
    describe_metric,
    describe_wowy_metric,
    describe_wowy_shrunk_metric,
)
from rawr_analytics.metrics.wowy.inputs import (
    WowyRequestDTO,
    WowySeasonInputDTO,
    build_wowy_request,
    validate_filters,
)
from rawr_analytics.metrics.wowy.records import (
    WowyPlayerSeasonRecord,
    WowyPlayerSeasonValue,
    build_player_season_records,
    build_wowy_custom_query,
    prepare_wowy_player_season_records,
)

__all__ = [
    "DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES",
    "WowyGame",
    "WowyPlayerSeasonRecord",
    "WowyPlayerSeasonValue",
    "WowyPlayerValue",
    "WowyRequestDTO",
    "WowySeasonInputDTO",
    "build_player_season_records",
    "build_wowy_custom_query",
    "build_wowy_request",
    "compute_wowy_shrinkage_score",
    "default_filters",
    "describe_metric",
    "describe_wowy_metric",
    "describe_wowy_shrunk_metric",
    "prepare_wowy_player_season_records",
    "validate_filters",
]
