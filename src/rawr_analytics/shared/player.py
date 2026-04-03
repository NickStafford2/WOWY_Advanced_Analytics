from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PlayerSummary:
    player_id: int
    player_name: str


@dataclass(frozen=True)
class PlayerMinutes:
    average_minutes: float | None
    total_minutes: float | None
