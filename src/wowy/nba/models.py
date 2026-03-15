from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NormalizedGameRecord:
    game_id: str
    season: str
    game_date: str
    team: str
    opponent: str
    is_home: bool
    margin: float
    season_type: str
    source: str


@dataclass(frozen=True)
class NormalizedGamePlayerRecord:
    game_id: str
    team: str
    player_id: int
    player_name: str
    appeared: bool
    minutes: float | None
