from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class NormalizedCacheLoadRow:
    team: Team
    season: Season
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
