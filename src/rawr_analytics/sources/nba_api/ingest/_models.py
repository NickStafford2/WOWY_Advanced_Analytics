from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.shared.game import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    NormalizedTeamSeasonBatch,
)
from rawr_analytics.shared.ingest import FetchError, IngestProgress, PartialTeamSeasonError
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

_TeamSeasonFailureError = FetchError | PartialTeamSeasonError | ValueError


@dataclass(frozen=True)
class IngestRequest:
    team: Team
    season: Season

    @property
    def label(self) -> str:
        return f"{self.team.abbreviation(season=self.season)} {self.season}"


@dataclass(frozen=True)
class IngestSummary:
    total_games: int
    processed_games: int
    fetched_box_scores: int
    cached_box_scores: int
    league_games_source: str


_TeamProgressFn = Callable[[IngestProgress], None]
IngestEventFn = Callable[["IngestEvent"], None]
FailureLogFn = Callable[["IngestFailureLogEntry"], None]


@dataclass(frozen=True)
class IngestSeasonStartedEvent:
    season_index: int
    season_total: int
    season: Season


@dataclass(frozen=True)
class IngestTeamProgressEvent:
    team_index: int
    team_total: int
    progress: IngestProgress


@dataclass(frozen=True)
class IngestResult:
    request: IngestRequest
    games: list[NormalizedGameRecord]
    game_players: list[NormalizedGamePlayerRecord]
    summary: IngestSummary

    def to_batch(self) -> NormalizedTeamSeasonBatch:
        return NormalizedTeamSeasonBatch(
            scope=TeamSeasonScope(team=self.request.team, season=self.request.season),
            games=self.games,
            game_players=self.game_players,
        )


@dataclass(frozen=True)
class IngestTeamCompletedEvent:
    team_index: int
    team_total: int
    result: IngestResult


@dataclass(frozen=True)
class SeasonRangeFailure:
    request: IngestRequest
    failure_kind: str
    error: _TeamSeasonFailureError

    @property
    def scope(self) -> str:
        return self.request.label

    def to_log_entry(self) -> IngestFailureLogEntry:
        return IngestFailureLogEntry(
            scope=self.scope,
            team=self.request.team,
            season=self.request.season,
            failure_kind=self.failure_kind,
            error=self.error,
        )


@dataclass(frozen=True)
class IngestTeamFailedEvent:
    team_index: int
    team_total: int
    failure: SeasonRangeFailure


IngestEvent = (
    IngestSeasonStartedEvent
    | IngestTeamProgressEvent
    | IngestTeamCompletedEvent
    | IngestTeamFailedEvent
)


@dataclass(frozen=True)
class SeasonRangeResult:
    seasons: list[Season]
    attempted_team_seasons: int
    completed_team_seasons: int
    failures: list[SeasonRangeFailure]

    @property
    def failure_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for failure in self.failures:
            counts[failure.failure_kind] = counts.get(failure.failure_kind, 0) + 1
        return counts

    @property
    def failed_scopes(self) -> list[str]:
        return [failure.scope for failure in self.failures]

    @property
    def exit_status(self) -> int:
        return 1 if self.failures else 0


@dataclass(frozen=True)
class IngestFailureLogEntry:
    scope: str
    team: Team
    season: Season
    failure_kind: str
    error: _TeamSeasonFailureError


__all__ = [
    "FailureLogFn",
    "IngestEvent",
    "IngestEventFn",
    "IngestFailureLogEntry",
    "IngestRequest",
    "IngestResult",
    "IngestSeasonStartedEvent",
    "IngestSummary",
    "IngestTeamCompletedEvent",
    "IngestTeamFailedEvent",
    "IngestTeamProgressEvent",
    "SeasonRangeFailure",
    "SeasonRangeResult",
    "_TeamProgressFn",
]
