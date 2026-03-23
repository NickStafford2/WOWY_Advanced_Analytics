from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class NbaIngestError(Exception):
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class FetchError(NbaIngestError):
    resource: str
    identifier: str
    attempts: int
    last_error_type: str
    last_error_message: str


@dataclass(frozen=True)
class LeagueGamesFetchError(FetchError):
    team: str
    season: str
    season_type: str


@dataclass(frozen=True)
class BoxScoreFetchError(FetchError):
    game_id: str


@dataclass(frozen=True)
class TeamSeasonConsistencyError(NbaIngestError):
    team: str
    season: str
    reason: str


@dataclass(frozen=True)
class GameNormalizationFailure:
    game_id: str
    error_type: str
    message: str


@dataclass(frozen=True)
class PartialTeamSeasonError(NbaIngestError):
    team: str
    season: str
    season_type: str
    failed_game_ids: list[str]
    total_games: int
    failed_games: int
    failed_game_details: list[GameNormalizationFailure] = field(default_factory=list)
    failure_reason_counts: dict[str, int] = field(default_factory=dict)
    failure_reason_examples: dict[str, list[str]] = field(default_factory=dict)
