from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrObservation:
    game_id: str
    game_date: str
    margin: float
    home_team: Team
    away_team: Team
    player_weights: dict[int, float]
    player_minutes: dict[int, float] | None = None


@dataclass(frozen=True)
class RawrPlayerContext:
    season: Season
    player_id: int
    player_name: str
    average_minutes: float | None
    total_minutes: float | None


@dataclass(frozen=True)
class RawrSeasonInput:
    season: Season
    observations: list[RawrObservation]
    players: list[RawrPlayerContext]


@dataclass(frozen=True)
class RawrRequest:
    season_inputs: list[RawrSeasonInput]
    min_games: int
    ridge_alpha: float
    shrinkage_mode: str = "uniform"
    shrinkage_strength: float = 1.0
    shrinkage_minute_scale: float = 48.0
    min_average_minutes: float | None = None
    min_total_minutes: float | None = None


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
class RawrCustomQueryRow:
    season_id: str
    player_id: int
    player_name: str
    coefficient: float
    games: int
    average_minutes: float | None
    total_minutes: float | None


@dataclass(frozen=True)
class RawrCustomQueryResult:
    metric: str
    metric_label: str
    rows: list[RawrCustomQueryRow]


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
