from __future__ import annotations

from typing import TypedDict


class GameRecord(TypedDict):
    game_id: str
    team: str
    margin: float
    players: set[int]


class PlayerStats(TypedDict):
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    wowy_score: float | None
