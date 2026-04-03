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
from rawr_analytics.nba import FetchError, PartialTeamSeasonError
from rawr_analytics.nba.errors import GameNormalizationFailure
from rawr_analytics.services.ingest import (
    IngestProgress,
    IngestRefreshRequest,
    IngestResult,
    SeasonRangeFailure,
    SeasonRangeResult,
    refresh_season_range,
)
from rawr_analytics.services.metric_refresh import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    MetricStoreRefreshRequest,
    RefreshMetricStoreResult,
    refresh_metric_store,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

ValidationProgressFn = Callable[[int, int, str], None]
MetricRefreshProgressFn = Callable[[str, int, int, str], None]
SeasonStartedFn = Callable[[int, int, str], None]
TeamCompletedFn = Callable[[int, int, IngestResult], None]
TeamFailedFn = Callable[["RebuildTeamFailureEvent"], None]
IngestProgressFn = Callable[[int, int, IngestProgress], None]


@dataclass(frozen=True)
class RebuildRequest:
    start_year: int
    end_year: int
    season_type: SeasonType
    metrics: list[Metric] | None = None
    teams: list[str] | None = None
    keep_existing_db: bool = False
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA


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


def format_rebuild_validation_summary(
    summary: DatabaseValidationSummary,
    *,
    top_n: int = 10,
) -> str:
    return render_rebuild_validation_summary(summary, top_n=top_n)


def rebuild_player_metrics_db(
    request: RebuildRequest,
    *,
    ingest_progress_fn: IngestProgressFn | None = None,
    season_started_fn: SeasonStartedFn | None = None,
    team_completed_fn: TeamCompletedFn | None = None,
    team_failed_fn: TeamFailedFn | None = None,
    metric_progress_fn: MetricRefreshProgressFn | None = None,
    validation_progress_fn: ValidationProgressFn | None = None,
) -> RebuildResult:
    deleted_existing_db = prepare_rebuild_storage(
        keep_existing_db=request.keep_existing_db
    )

    ingest_result = refresh_season_range(
        IngestRefreshRequest(
            start_year=request.start_year,
            end_year=request.end_year,
            season_type=request.season_type.value,
            team_abbreviations=request.teams,
        ),
        progress_fn=ingest_progress_fn,
        season_started_fn=(
            None
            if season_started_fn is None
            else lambda season_index, season_total, season: season_started_fn(
                season_index,
                season_total,
                str(season),
            )
        ),
        team_completed_fn=team_completed_fn,
        team_failed_fn=(
            None
            if team_failed_fn is None
            else lambda team_index, team_total, failure: team_failed_fn(
                _build_rebuild_team_failure_event(
                    team_index=team_index,
                    team_total=team_total,
                    failure=failure,
                )
            )
        ),
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
    for metric in request.metrics or [Metric.WOWY, Metric.WOWY_SHRUNK, Metric.RAWR]:
        result = refresh_metric_store(
            MetricStoreRefreshRequest(
                metric=metric,
                season_type=request.season_type,
                rawr_ridge_alpha=request.rawr_ridge_alpha,
                include_team_scopes=False,
            ),
            progress=(
                None
                if metric_progress_fn is None
                else lambda current, total, detail, metric=metric: metric_progress_fn(
                    metric.value,
                    current,
                    total,
                    detail,
                )
            ),
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

    validation_summary = validate_rebuild_storage(progress=validation_progress_fn)
    failure_message = None if validation_summary.ok else "Database validation failed after rebuild."
    return RebuildResult(
        ingest_result=ingest_result,
        metric_results=metric_results,
        validation_summary=validation_summary,
        deleted_existing_db=deleted_existing_db,
        failure_message=failure_message,
    )


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
    "RebuildRequest",
    "RebuildResult",
    "RebuildTeamFailureEvent",
    "format_rebuild_validation_summary",
    "rebuild_player_metrics_db",
]
