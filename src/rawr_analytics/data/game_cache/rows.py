from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NormalizedCacheLoadRow:
    team: str
    team_id: int
    season: str
    season_type: str
    source_path: str
    source_snapshot: str
    source_kind: str
    build_version: str
    refreshed_at: str
    games_row_count: int
    game_players_row_count: int
    expected_games_row_count: int | None = None
    skipped_games_row_count: int | None = None


__all__ = ["NormalizedCacheLoadRow"]
