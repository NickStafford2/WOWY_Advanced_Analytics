from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class NormalizedGameRecord:
    game_id: str
    game_date: str
    season: Season
    team: Team
    opponent_team: Team
    is_home: bool
    margin: float
    source: str


@dataclass(frozen=True)
class NormalizedGamePlayerRecord:
    game_id: str
    player_id: int
    player_name: str
    appeared: bool
    minutes: float | None
    team: Team
    season: Season


@dataclass(frozen=True)
class NormalizedTeamSeasonBatch:
    team: Team
    season: Season
    games: list[NormalizedGameRecord]
    game_players: list[NormalizedGamePlayerRecord]
