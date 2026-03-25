"""Public package API for canonical NBA normalization."""

from __future__ import annotations

from wowy.nba.normalize.models import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    NormalizedTeamSeasonBatch,
)
from wowy.nba.normalize.normalize_game import normalize_source_game
from wowy.nba.normalize.validation import (
    validate_normalized_cache_batch,
    validate_normalized_team_season_batch,
)

__all__ = [
    "NormalizedGamePlayerRecord",
    "NormalizedGameRecord",
    "NormalizedTeamSeasonBatch",
    "normalize_source_game",
    "validate_normalized_cache_batch",
    "validate_normalized_team_season_batch",
]
