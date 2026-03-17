from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RawrObservation:
    game_id: str
    season: str
    game_date: str
    home_team: str
    away_team: str
    margin: float
    player_weights: dict[int, float]


@dataclass(frozen=True)
class RawrPlayerEstimate:
    player_id: int
    player_name: str
    games: int
    average_minutes: float | None
    total_minutes: float | None
    coefficient: float


@dataclass(frozen=True)
class RawrPlayerSeasonRecord:
    season: str
    player_id: int
    player_name: str
    games: int
    average_minutes: float | None
    total_minutes: float | None
    coefficient: float


@dataclass(frozen=True)
class RawrResult:
    observations: int
    players: int
    intercept: float
    home_court_advantage: float
    estimates: list[RawrPlayerEstimate]


@dataclass(frozen=True)
class RawrModel:
    player_ids: list[int]
    team_seasons: list[str]
    coefficients: list[float]


@dataclass(frozen=True)
class RidgeTuningResult:
    alpha: float
    validation_mse: float


@dataclass(frozen=True)
class RidgeTuningSummary:
    best_alpha: float
    results: list[RidgeTuningResult]
