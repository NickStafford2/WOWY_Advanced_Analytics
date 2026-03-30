from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrObservation:
    game_id: str
    season: Season
    game_date: str
    margin: float
    home_team: Team
    away_team: Team
    player_weights: dict[int, float]
    player_minutes: dict[int, float] | None = None


@dataclass(frozen=True)
class RawrPlayerEstimate:
    season: Season
    player_id: int
    player_name: str
    games: int
    average_minutes: float | None
    total_minutes: float | None
    coefficient: float


@dataclass(frozen=True)
class RawrPlayerSeasonRecord:
    season: Season
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
    player_keys: list[tuple[Season, int]]
    team_seasons: list[tuple[Team, Season]]
    coefficients: list[float]


@dataclass(frozen=True)
class RidgeTuningResult:
    alpha: float
    validation_mse: float


@dataclass(frozen=True)
class RidgeTuningSummary:
    best_alpha: float
    results: list[RidgeTuningResult]
