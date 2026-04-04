from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data import (
    DatabaseValidationSummary,
    prepare_rebuild_storage,
    render_rebuild_validation_summary,
    validate_rebuild_storage,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.services.metric_refresh import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    MetricStoreRefreshProgressEvent,
    RefreshMetricStoreResult,
    refresh_metric_store,
)
from rawr_analytics.shared.ingest import (
    FetchError,
    GameNormalizationFailure,
    PartialTeamSeasonError,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team
from rawr_analytics.sources.nba_api.ingest import (
    FailureLogFn,
    IngestEvent,
    IngestSeasonStartedEvent,
    IngestTeamCompletedEvent,
    IngestTeamFailedEvent,
    IngestTeamProgressEvent,
    SeasonRangeFailure,
    SeasonRangeResult,
    refresh_season_range,
)

RebuildEventFn = Callable[["RebuildEvent"], None]


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


def format_rebuild_validation_summary(
    summary: DatabaseValidationSummary,
    *,
    top_n: int = 10,
) -> str:
    return render_rebuild_validation_summary(summary, top_n=top_n)


def rebuild_player_metrics_db(
    *,
    start_year: int,
    end_year: int,
    season_type: str,
    teams: list[str] | None,
    metrics: list[str] | None,
    keep_existing_db: bool,
    event_fn: RebuildEventFn | None = None,
    failure_log_fn: FailureLogFn | None = None,
) -> RebuildResult:
    if start_year < end_year:
        raise ValueError("Start year must be greater than or equal to end year")

    normalized_season_type = SeasonType.parse(season_type)
    normalized_metrics = [Metric.parse(metric) for metric in metrics] if metrics else None
    deleted_existing_db = prepare_rebuild_storage(keep_existing_db=keep_existing_db)

    def _emit_ingest_event(event: IngestEvent) -> None:
        assert event_fn is not None
        _forward_rebuild_ingest_event(event_fn, event)

    ingest_result = refresh_season_range(
        start_year=start_year,
        end_year=end_year,
        season_type=normalized_season_type.value,
        team_abbreviations=teams,
        event_fn=None if event_fn is None else _emit_ingest_event,
        failure_log_fn=failure_log_fn,
    )
    if ingest_result.failures:
        return RebuildResult(
            ingest_result=ingest_result,
            metric_results=[],
            validation_summary=None,
            deleted_existing_db=deleted_existing_db,
            failure_message="Ingest refresh failed during rebuild.",
        )

    metric_results: list[RefreshMetricStoreResult] = []
    for metric in normalized_metrics or [Metric.WOWY, Metric.WOWY_SHRUNK, Metric.RAWR]:

        def _emit_metric_refresh_event(
            event: MetricStoreRefreshProgressEvent,
            *,
            _metric: Metric = metric,
        ) -> None:
            assert event_fn is not None
            event_fn(
                RebuildMetricRefreshProgressEvent(
                    metric=_metric.value,
                    current=event.current,
                    total=event.total,
                    detail=event.detail,
                )
            )

        result = refresh_metric_store(
            metric=metric,
            season_type=normalized_season_type,
            rawr_ridge_alpha=DEFAULT_RAWR_RIDGE_ALPHA,
            include_team_scopes=False,
            event_fn=None if event_fn is None else _emit_metric_refresh_event,
        )
        metric_results.append(result)
        if not result.ok:
            return RebuildResult(
                ingest_result=ingest_result,
                metric_results=metric_results,
                validation_summary=None,
                deleted_existing_db=deleted_existing_db,
                failure_message=result.failure_message,
            )

    def _emit_validation_progress(current: int, total: int, label: str) -> None:
        assert event_fn is not None
        event_fn(
            RebuildValidationProgressEvent(
                current=current,
                total=total,
                label=label,
            )
        )

    validation_summary = validate_rebuild_storage(
        progress=None if event_fn is None else _emit_validation_progress
    )
    failure_message = None if validation_summary.ok else "Database validation failed after rebuild."
    return RebuildResult(
        ingest_result=ingest_result,
        metric_results=metric_results,
        validation_summary=validation_summary,
        deleted_existing_db=deleted_existing_db,
        failure_message=failure_message,
    )


def _forward_rebuild_ingest_event(
    event_fn: RebuildEventFn,
    event: IngestEvent,
) -> None:
    if isinstance(event, IngestTeamFailedEvent):
        event_fn(
            _build_rebuild_team_failure_event(
                team_index=event.team_index,
                team_total=event.team_total,
                failure=event.failure,
            )
        )
        return
    event_fn(event)


def _build_rebuild_team_failure_event(
    *,
    team_index: int,
    team_total: int,
    failure: SeasonRangeFailure,
) -> RebuildTeamFailureEvent:
    request = failure.request
    team = request.team
    season = request.season
    error = failure.error
    team_label = team.abbreviation(season=season)
    season_label = str(season)

    if failure.failure_kind == "fetch_error":
        assert isinstance(error, FetchError)
        return RebuildTeamFailureEvent(
            team_index=team_index,
            team_total=team_total,
            scope=failure.scope,
            team=team,
            season=season,
            team_label=team_label,
            season_label=season_label,
            failure_kind=failure.failure_kind,
            error=error,
            fetch_error_type=error.last_error_type,
            failed_games=None,
            total_games=None,
            reason=str(error),
        )

    if failure.failure_kind == "partial_scope_error":
        assert isinstance(error, PartialTeamSeasonError)
        return RebuildTeamFailureEvent(
            team_index=team_index,
            team_total=team_total,
            scope=failure.scope,
            team=team,
            season=season,
            team_label=team_label,
            season_label=season_label,
            failure_kind=failure.failure_kind,
            error=error,
            fetch_error_type=None,
            failed_games=error.failed_games,
            total_games=error.total_games,
            reason=str(error),
            failed_game_details=list(error.failed_game_details),
            failure_reason_counts=dict(sorted(error.failure_reason_counts.items())),
            failure_reason_examples={
                reason: examples[:]
                for reason, examples in sorted(error.failure_reason_examples.items())
            },
        )

    return RebuildTeamFailureEvent(
        team_index=team_index,
        team_total=team_total,
        scope=failure.scope,
        team=team,
        season=season,
        team_label=team_label,
        season_label=season_label,
        failure_kind=failure.failure_kind,
        error=error,
        fetch_error_type=None,
        failed_games=None,
        total_games=None,
        reason=str(error),
    )


__all__ = [
    "RebuildEvent",
    "RebuildEventFn",
    "RebuildMetricRefreshProgressEvent",
    "RebuildResult",
    "RebuildTeamFailureEvent",
    "RebuildValidationProgressEvent",
    "format_rebuild_validation_summary",
    "rebuild_player_metrics_db",
]
