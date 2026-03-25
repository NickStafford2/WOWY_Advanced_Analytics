"""Public package API for normalized game-cache operations."""

from __future__ import annotations

from wowy.data.game_cache.fingerprints import build_normalized_cache_fingerprint
from wowy.data.game_cache.repository import (
    list_cache_load_rows,
    list_cached_team_seasons_from_db,
    load_cache_load_row,
    load_normalized_game_players_from_db,
    load_normalized_games_from_db,
    replace_team_season_normalized_rows,
)
from wowy.data.game_cache.rows import NormalizedCacheLoadRow
from wowy.data.game_cache.schema import initialize_game_cache_db

__all__ = [
    "NormalizedCacheLoadRow",
    "build_normalized_cache_fingerprint",
    "initialize_game_cache_db",
    "list_cache_load_rows",
    "list_cached_team_seasons_from_db",
    "load_cache_load_row",
    "load_normalized_game_players_from_db",
    "load_normalized_games_from_db",
    "replace_team_season_normalized_rows",
]
