from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegressionObservation:
    game_id: str
    season: str
    game_date: str
    home_team: str
    away_team: str
    margin: float
    player_weights: dict[int, float]


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
    home_court_advantage: float
    estimates: list[RegressionPlayerEstimate]
