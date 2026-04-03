from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.player import PlayerSummary
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class SourceLeagueGame:
    game_id: str
    game_date: str
    matchup: str
    team: Team
    raw_row: dict[str, object]  # todo: see what sort of object this can be


@dataclass(frozen=True)
class SourceLeagueSchedule:
    scope: TeamSeasonScope
    games: list[SourceLeagueGame]


@dataclass(frozen=True)
class SourceBoxScorePlayer:
    game_id: str
    team: Team
    player: PlayerSummary | None
    minutes_raw: str | int | None
    raw_row: dict[str, object]  # todo: see what sort of object this can be


@dataclass(frozen=True)
class SourceBoxScoreTeam:
    team: Team
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
