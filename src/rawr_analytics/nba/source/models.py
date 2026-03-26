from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceLeagueGame:
    game_id: str
    game_date: str
    matchup: str
    team_id: int
    team_abbreviation: str
    raw_row: dict[str, object]  # todo: see what sort of object this can be


@dataclass(frozen=True)
class SourceLeagueSchedule:
    requested_team: str
    season: str
    season_type: str
    games: list[SourceLeagueGame]


@dataclass(frozen=True)
class SourceBoxScorePlayer:
    game_id: str
    team_id: int | None
    team_abbreviation: str
    player_id: int | None
    player_name: str
    minutes_raw: str | int | None
    raw_row: dict[str, object]  # todo: see what sort of object this can be


@dataclass(frozen=True)
class SourceBoxScoreTeam:
    team_id: int | None
    team_abbreviation: str
    plus_minus_raw: int | float | None  # todo: see what sort of object this can be
    points_raw: object  # todo: see what sort of object this can be
    raw_row: dict[str, object]  # todo: see what sort of object this can be


@dataclass(frozen=True)
class SourceBoxScore:
    game_id: str
    players: list[SourceBoxScorePlayer]
    teams: list[SourceBoxScoreTeam]


__all__ = [
    "SourceBoxScore",
    "SourceBoxScorePlayer",
    "SourceBoxScoreTeam",
    "SourceLeagueGame",
    "SourceLeagueSchedule",
]
