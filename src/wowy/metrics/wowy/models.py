from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WowyGameRecord:
    game_id: str
    season: str
    team: str
    margin: float
    players: set[int]
    team_id: int | None = None

    @property
    def identity_team(self) -> int | str:
        return self.team_id if self.team_id is not None else self.team


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
    season: str
    player_id: int
    player_name: str
    games_with: int
    games_without: int
    avg_margin_with: float
    avg_margin_without: float
    wowy_score: float
    average_minutes: float | None
    total_minutes: float | None
