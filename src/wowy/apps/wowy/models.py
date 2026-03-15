from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WowyGameRecord:
    game_id: str
    season: str
    team: str
    margin: float
    players: set[int]


@dataclass(frozen=True)
class WowyPlayerStats:
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    wowy_score: float | None
    average_minutes: float | None = None
    total_minutes: float | None = None
