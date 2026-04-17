"""Public package API for normalized game-cache operations.
- Stores and reads the normalized cache DB.
- This is where ingested source data lives after normalization.
"""

from __future__ import annotations

from rawr_analytics.data.game_cache.store import (
    list_cached_scopes,
    load_game_cache_snapshot,
    load_games_for_team_season_scopes,
    store_team_season_cache,
)

__all__ = [
    "list_cached_scopes",
    "load_game_cache_snapshot",
    "load_games_for_team_season_scopes",
    "store_team_season_cache",
]
