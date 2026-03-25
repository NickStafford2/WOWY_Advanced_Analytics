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
    team_id: int
    opponent_team_id: int

    @property
    def identity_team(self) -> int:
        return self.team_id

    @property
    def identity_opponent(self) -> int:
        return self.opponent_team_id


@dataclass(frozen=True)
class NormalizedGamePlayerRecord:
    game_id: str
    team: str
    player_id: int
    player_name: str
    appeared: bool
    minutes: float | None
    team_id: int

    @property
    def identity_team(self) -> int:
        return self.team_id


@dataclass(frozen=True)
class NormalizedTeamSeasonBatch:
    team: str
    team_id: int
    season: str
    season_type: str
    games: list[NormalizedGameRecord]
    game_players: list[NormalizedGamePlayerRecord]
