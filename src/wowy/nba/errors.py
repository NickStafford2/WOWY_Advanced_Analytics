from __future__ import annotations

from dataclasses import dataclass


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
