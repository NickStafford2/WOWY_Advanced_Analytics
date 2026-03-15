from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GameRecord:
    game_id: str
    team: str
    margin: float
    players: set[int]


@dataclass(frozen=True)
class PlayerStats:
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    wowy_score: float | None
