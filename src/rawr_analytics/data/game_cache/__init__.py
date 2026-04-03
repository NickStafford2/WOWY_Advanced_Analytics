"""Public package API for normalized game-cache operations."""

from __future__ import annotations

from rawr_analytics.data.game_cache._repository import (
    list_cache_load_rows,
    list_cached_team_seasons,
    load_normalized_scope_records_from_db,
    replace_team_season_normalized_rows,
)
from rawr_analytics.data.game_cache._validation import (
    validate_normalized_cache_loads_table,
    validate_normalized_cache_relations,
    validate_normalized_game_players_table,
    validate_normalized_games_table,
)
from rawr_analytics.data.game_cache.fingerprints import build_normalized_cache_fingerprint
from rawr_analytics.data.game_cache.rows import (
    NormalizedCacheLoadRow,
    NormalizedGamePlayerRow,
    NormalizedGameRow,
)
from rawr_analytics.data.game_cache.schema import initialize_game_cache_db

__all__ = [
    "NormalizedCacheLoadRow",
    "NormalizedGamePlayerRow",
    "NormalizedGameRow",
    "build_normalized_cache_fingerprint",
    "initialize_game_cache_db",
    "list_cache_load_rows",
    "list_cached_team_seasons",
    "load_normalized_scope_records_from_db",
    "replace_team_season_normalized_rows",
    "validate_normalized_cache_loads_table",
    "validate_normalized_cache_relations",
    "validate_normalized_game_players_table",
    "validate_normalized_games_table",
]
