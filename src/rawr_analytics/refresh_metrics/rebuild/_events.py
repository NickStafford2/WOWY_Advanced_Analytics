from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.audit.reporting import DatabaseValidationSummary
from rawr_analytics.refresh_metrics.refresh_metric_store import RefreshMetricStoreResult
from rawr_analytics.shared.ingest import (
    FetchError,
    GameNormalizationFailure,
    IngestProgress,
    PartialTeamSeasonError,
)
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

RebuildFailureError = FetchError | PartialTeamSeasonError | ValueError


@dataclass(frozen=True)
class RebuildIngestFailure:
    team: Team
    season: Season
    failure_kind: str
    error: RebuildFailureError

    @property
    def scope(self) -> str:
        return f"{self.team.abbreviation(season=self.season)} {self.season}"


RebuildFailureLogFn = Callable[[RebuildIngestFailure], None]


@dataclass(frozen=True)
class RebuildIngestResult:
    attempted_team_seasons: int
    completed_team_seasons: int
    failures: list[RebuildIngestFailure]

    @property
    def failure_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for failure in self.failures:
            counts[failure.failure_kind] = counts.get(failure.failure_kind, 0) + 1
        return counts

    @property
    def failed_scopes(self) -> list[str]:
        return [failure.scope for failure in self.failures]


@dataclass(frozen=True)
class RebuildSeasonStartedEvent:
    season_index: int
    season_total: int
    season_label: str


@dataclass(frozen=True)
class RebuildTeamProgressEvent:
    team_index: int
    team_total: int
    progress: IngestProgress


@dataclass(frozen=True)
class RebuildTeamCompletedEvent:
    team_index: int
    team_total: int
    team_label: str
    season_label: str
    processed_games: int
    total_games: int
    league_games_source: str
    fetched_box_scores: int
    cached_box_scores: int


@dataclass(frozen=True)
class RebuildResult:
    ingest_result: RebuildIngestResult
    metric_results: list[RefreshMetricStoreResult]
    validation_summary: DatabaseValidationSummary | None
    deleted_existing_db: bool
    failure_message: str | None = None

    @property
    def ok(self) -> bool:
        return self.failure_message is None


@dataclass(frozen=True)
class RebuildTeamFailureEvent:
    team_index: int
    team_total: int
    scope: str
    team: Team
    season: Season
    team_label: str
    season_label: str
    failure_kind: str
    error: RebuildFailureError
    fetch_error_type: str | None
    failed_games: int | None
    total_games: int | None
    reason: str
    failed_game_details: list[GameNormalizationFailure] | None = None
    failure_reason_counts: dict[str, int] | None = None
    failure_reason_examples: dict[str, list[str]] | None = None


@dataclass(frozen=True)
class RebuildMetricRefreshProgressEvent:
    metric: str
    current: int
    total: int
    detail: str


@dataclass(frozen=True)
class RebuildValidationProgressEvent:
    current: int
    total: int
    label: str


RebuildEvent = (
    RebuildSeasonStartedEvent
    | RebuildTeamProgressEvent
    | RebuildTeamCompletedEvent
    | RebuildTeamFailureEvent
    | RebuildMetricRefreshProgressEvent
    | RebuildValidationProgressEvent
)

RebuildEventFn = Callable[[RebuildEvent], None]
