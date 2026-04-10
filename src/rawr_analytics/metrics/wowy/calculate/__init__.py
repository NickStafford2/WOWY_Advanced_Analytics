"""WOWY calculation workflow.

Inputs are normalized game and player records grouped by season. Outputs are
typed WOWY player-season records and validation errors for invalid filters.
"""

from rawr_analytics.metrics.wowy.calculate.inputs import (
    WowyRequestDTO,
    WowySeasonInputDTO,
    build_wowy_request,
    validate_filters,
    validate_request,
)
from rawr_analytics.metrics.wowy.calculate.records import (
    WowyPlayerSeasonRecord,
    WowyPlayerSeasonValue,
    build_player_season_records,
    build_wowy_custom_query,
    build_wowy_player_season_value,
    prepare_wowy_player_season_records,
)
from rawr_analytics.metrics.wowy.calculate.shrinkage import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)

__all__ = [
    "DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES",
    "WowyPlayerSeasonRecord",
    "WowyPlayerSeasonValue",
    "WowyRequestDTO",
    "WowySeasonInputDTO",
    "build_player_season_records",
    "build_wowy_custom_query",
    "build_wowy_player_season_value",
    "build_wowy_request",
    "compute_wowy_shrinkage_score",
    "prepare_wowy_player_season_records",
    "validate_filters",
    "validate_request",
]
