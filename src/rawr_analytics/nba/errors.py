from __future__ import annotations

from dataclasses import dataclass, field

from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass
class NbaIngestError(Exception):
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass
class FetchError(NbaIngestError):
    resource: str
    identifier: str
    attempts: int
    last_error_type: str
    last_error_message: str


@dataclass
class LeagueGamesFetchError(FetchError):
    team: Team
    season: Season


@dataclass
class BoxScoreFetchError(FetchError):
    game_id: str


@dataclass
class GameNormalizationFailure:
    game_id: str
    error_type: str
    message: str


@dataclass
class PartialTeamSeasonError(NbaIngestError):
    team: Team
    season: Season
    failed_game_ids: list[str]
    total_games: int
    failed_games: int
    failed_game_details: list[GameNormalizationFailure] = field(default_factory=list)
    failure_reason_counts: dict[str, int] = field(default_factory=dict)
    failure_reason_examples: dict[str, list[str]] = field(default_factory=dict)
