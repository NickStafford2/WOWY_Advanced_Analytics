from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class KaggleIngestScopeSummary:
    team_id: int
    season_id: str
    season_type: str
    games: int
    game_players: int
    skipped_games: int


@dataclass(frozen=True)
class KaggleIngestResult:
    source_root: Path
    games_path: Path
    player_statistics_path: Path
    source_snapshot: str
    scope_count: int
    game_count: int
    game_player_count: int
    skipped_game_count: int
    skipped_game_types: tuple[str, ...]
    scopes: tuple[KaggleIngestScopeSummary, ...]


__all__ = [
    "KaggleIngestResult",
    "KaggleIngestScopeSummary",
]
