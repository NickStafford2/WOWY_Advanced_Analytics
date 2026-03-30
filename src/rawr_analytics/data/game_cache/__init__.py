"""Public package API for normalized game-cache operations."""

from __future__ import annotations

from rawr_analytics.data.game_cache.fingerprints import build_normalized_cache_fingerprint
from rawr_analytics.data.game_cache.repository import (
    list_cache_load_rows,
    list_cached_team_seasons,
    replace_team_season_normalized_rows,
)
from rawr_analytics.data.game_cache.rows import NormalizedCacheLoadRow

__all__ = [
    "NormalizedCacheLoadRow",
    "build_normalized_cache_fingerprint",
    "list_cache_load_rows",
    "list_cached_team_seasons",
    "replace_team_season_normalized_rows",
]
