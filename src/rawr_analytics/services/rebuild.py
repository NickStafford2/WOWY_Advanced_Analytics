from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data._paths import (
    LEGACY_MIXED_DATA_DB_PATH,
    METRIC_STORE_DB_PATH,
    NORMALIZED_CACHE_DB_PATH,
)
from rawr_analytics.data.db_validation import (
    DatabaseValidationSummary,
    audit_player_metrics_db,
    summarize_validation_report,
)
from rawr_analytics.metrics.constants import Metric
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

ValidationProgressFn = Callable[[int, int, str], None]
MetricRefreshProgressFn = Callable[[Metric, int, int, str], None]
SeasonStartedFn = Callable[[int, int, Season], None]
TeamCompletedFn = Callable[[int, int, IngestResult], None]
TeamFailedFn = Callable[[int, int, SeasonRangeFailure], None]
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
    deleted_existing_db = False
    if not request.keep_existing_db:
        for db_path in (
            NORMALIZED_CACHE_DB_PATH,
            METRIC_STORE_DB_PATH,
            LEGACY_MIXED_DATA_DB_PATH,
        ):
            if db_path.exists():
                db_path.unlink()
                deleted_existing_db = True
    NORMALIZED_CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    ingest_result = refresh_season_range(
        IngestRefreshRequest(
            start_year=request.start_year,
            end_year=request.end_year,
            season_type=request.season_type.value,
            team_abbreviations=request.teams,
        ),
        progress_fn=ingest_progress_fn,
        season_started_fn=season_started_fn,
        team_completed_fn=team_completed_fn,
        team_failed_fn=team_failed_fn,
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
                    metric,
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

    validation_report = audit_player_metrics_db(progress=validation_progress_fn)
    validation_summary = summarize_validation_report(validation_report)
    failure_message = None if validation_summary.ok else "Database validation failed after rebuild."
    return RebuildResult(
        ingest_result=ingest_result,
        metric_results=metric_results,
        validation_summary=validation_summary,
        deleted_existing_db=deleted_existing_db,
        failure_message=failure_message,
    )


__all__ = [
    "RebuildRequest",
    "RebuildResult",
    "rebuild_player_metrics_db",
]
