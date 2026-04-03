"""Public package API for normalized game-cache operations."""

from __future__ import annotations

from rawr_analytics.data.game_cache.fingerprints import build_normalized_cache_fingerprint
from rawr_analytics.data.game_cache.repository import (
    list_cache_load_rows,
    list_cached_team_seasons,
    load_normalized_scope_records_from_db,
    replace_team_season_normalized_rows,
)
from rawr_analytics.data.game_cache.rows import (
    NormalizedCacheLoadRow,
    NormalizedGamePlayerRow,
    NormalizedGameRow,
)

__all__ = [
    "NormalizedCacheLoadRow",
    "NormalizedGamePlayerRow",
    "NormalizedGameRow",
    "build_normalized_cache_fingerprint",
    "list_cache_load_rows",
    "list_cached_team_seasons",
    "load_normalized_scope_records_from_db",
    "replace_team_season_normalized_rows",
]
