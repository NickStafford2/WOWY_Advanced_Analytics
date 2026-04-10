"""RAWR calculation workflow.

Inputs are normalized game and player records grouped by season. Outputs are
typed RAWR player-season records and validation errors for invalid filters.
"""

from rawr_analytics.metrics.rawr.calculate.inputs import (
    RawrEligibility,
    RawrRequestDTO,
    RawrSeasonInputDTO,
    build_rawr_request,
    validate_filters,
    validate_request,
)
from rawr_analytics.metrics.rawr.calculate.records import (
    RawrPlayerSeasonRecord,
    build_player_season_records,
)
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode

__all__ = [
    "RawrEligibility",
    "RawrPlayerSeasonRecord",
    "RawrRequestDTO",
    "RawrSeasonInputDTO",
    "RawrShrinkageMode",
    "build_player_season_records",
    "build_rawr_request",
    "validate_filters",
    "validate_request",
]
