from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class WowyGameRecord:
    game_id: str
    season: Season
    margin: float
    players: set[int]
    team: Team


@dataclass(frozen=True)
class WowyPlayerStats:
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    wowy_score: float | None
    average_minutes: float | None = None
    total_minutes: float | None = None


@dataclass(frozen=True)
class WowyPlayerSeasonRecord:
    season: Season
    player_id: int
    player_name: str
    games_with: int
    games_without: int
    avg_margin_with: float
    avg_margin_without: float
    wowy_score: float
    average_minutes: float | None
    total_minutes: float | None
