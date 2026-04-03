from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class WowyGame:
    game_id: str
    margin: float
    players: frozenset[int]
    team: Team


@dataclass(frozen=True)
class WowyPlayerContext:
    player: PlayerSummary
    minutes: PlayerMinutes


@dataclass(frozen=True)
class WowySeasonInput:
    season: Season
    games: list[WowyGame]
    players: list[WowyPlayerContext]


@dataclass(frozen=True)
class WowyRequest:
    season_inputs: list[WowySeasonInput]
    min_games_with: int
    min_games_without: int
    min_average_minutes: float | None = None
    min_total_minutes: float | None = None


@dataclass(frozen=True)
class WowyPlayerValue:
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    value: float | None
    raw_value: float | None = None


@dataclass(frozen=True)
class WowyPlayerSeasonRecord:
    season: Season
    player: PlayerSummary
    minutes: PlayerMinutes
    result: WowyPlayerValue


@dataclass(frozen=True)
class WowyPlayerSeasonValue:
    season_id: str
    player: PlayerSummary
    minutes: PlayerMinutes
    result: WowyPlayerValue


@dataclass(frozen=True)
class WowyCustomQueryResult:
    metric: str
    metric_label: str
    rows: list[WowyPlayerSeasonValue]
