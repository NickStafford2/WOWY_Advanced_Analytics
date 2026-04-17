"""RAWR calculation workflow.

Inputs are normalized game and player records grouped by season. Outputs are
typed RAWR player-season records and validation errors for invalid filters.
"""

from rawr_analytics.metrics.rawr.calculate.inputs import build_rawr_request
from rawr_analytics.metrics.rawr.calculate.records import (
    RawrPlayerSeasonRecord,
    build_player_season_records,
)

__all__ = [
    "RawrPlayerSeasonRecord",
    "build_player_season_records",
    "build_rawr_request",
]
