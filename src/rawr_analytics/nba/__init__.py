"""Owns canonical basketball-domain logic.

Examples:

- source payload parsing
- normalization into canonical game/player/team records
- canonical validation
- team identity and historical continuity rules
"""

from rawr_analytics.nba.errors import (
    FetchError,
    PartialTeamSeasonError,
)
from rawr_analytics.nba.ingest_logging import append_ingest_failure_log
from rawr_analytics.nba.player_participation import (
    has_positive_minutes,
    player_has_positive_minutes,
)

__all__ = [
    "FetchError",
    "PartialTeamSeasonError",
    "append_ingest_failure_log",
    "has_positive_minutes",
    "player_has_positive_minutes",
]
