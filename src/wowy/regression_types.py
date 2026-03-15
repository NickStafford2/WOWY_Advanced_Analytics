from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegressionObservation:
    game_id: str
    season: str
    game_date: str
    team: str
    opponent: str
    is_home: bool
    margin: float
    player_ids: set[int]


@dataclass(frozen=True)
class RegressionPlayerEstimate:
    player_id: int
    player_name: str
    games: int
    coefficient: float


@dataclass(frozen=True)
class RegressionResult:
    observations: int
    players: int
    intercept: float
    estimates: list[RegressionPlayerEstimate]
