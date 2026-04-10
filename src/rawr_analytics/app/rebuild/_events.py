from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.app.refresh_metrics import RefreshMetricStoreResult
from rawr_analytics.data.db_validation import DatabaseValidationSummary
from rawr_analytics.shared.ingest import (
    FetchError,
    GameNormalizationFailure,
    PartialTeamSeasonError,
)
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team
from rawr_analytics.sources.nba_api.ingest._models import (
    IngestSeasonStartedEvent,
    IngestTeamCompletedEvent,
    IngestTeamProgressEvent,
    SeasonRangeResult,
)


@dataclass(frozen=True)
class RebuildResult:
    ingest_result: SeasonRangeResult
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
    error: FetchError | PartialTeamSeasonError | ValueError
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
    IngestSeasonStartedEvent
    | IngestTeamProgressEvent
    | IngestTeamCompletedEvent
    | RebuildTeamFailureEvent
    | RebuildMetricRefreshProgressEvent
    | RebuildValidationProgressEvent
)

RebuildEventFn = Callable[[RebuildEvent], None]
