from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CanonicalGameRecord:
    game_id: str
    season: str
    game_date: str
    team: str
    opponent: str
    is_home: bool
    margin: float
    season_type: str
    source: str
    team_id: int | None = None
    opponent_team_id: int | None = None

    @property
    def identity_team(self) -> int | str:
        return self.team_id if self.team_id is not None else self.team

    @property
    def identity_opponent(self) -> int | str:
        return (
            self.opponent_team_id
            if self.opponent_team_id is not None
            else self.opponent
        )


@dataclass(frozen=True)
class CanonicalGamePlayerRecord:
    game_id: str
    team: str
    player_id: int
    player_name: str
    appeared: bool
    minutes: float | None
    team_id: int | None = None

    @property
    def identity_team(self) -> int | str:
        return self.team_id if self.team_id is not None else self.team


@dataclass(frozen=True)
class CanonicalTeamSeasonBatch:
    team: str
    team_id: int
    season: str
    season_type: str
    games: list[CanonicalGameRecord]
    game_players: list[CanonicalGamePlayerRecord]


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
