from __future__ import annotations

from rawr_analytics.data.prepare_rebuild import prepare_rebuild_storage
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.refresh_metrics.rebuild._events import (
    RebuildEventFn,
    RebuildFailureLogFn,
    RebuildResult,
)
from rawr_analytics.refresh_metrics.rebuild._ingest import run_ingest
from rawr_analytics.refresh_metrics.rebuild._metric_refresh import (
    resolve_metrics,
    run_metric_refresh,
)
from rawr_analytics.refresh_metrics.rebuild._validation import validate_rebuild_result
from rawr_analytics.shared.season import SeasonType


def rebuild_player_metrics_db(
    *,
    start_year: int,
    end_year: int,
    season_type: str,
    teams: list[str] | None,
    metrics: list[str] | None,
    keep_existing_db: bool,
    event_fn: RebuildEventFn | None = None,
    failure_log_fn: RebuildFailureLogFn | None = None,
) -> RebuildResult:
    if start_year < end_year:
        raise ValueError("Start year must be greater than or equal to end year")

    normalized_season_type = SeasonType.parse(season_type)
    normalized_metrics = [Metric.parse(metric) for metric in metrics] if metrics else None
    deleted_existing_db = prepare_rebuild_storage(keep_existing_db=keep_existing_db)

    ingest_result = run_ingest(
        start_year=start_year,
        end_year=end_year,
        season_type=normalized_season_type.value,
        teams=teams,
        event_fn=event_fn,
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

    metric_results = run_metric_refresh(
        metrics=resolve_metrics(normalized_metrics),
        season_type=normalized_season_type,
        event_fn=event_fn,
    )
    for result in metric_results:
        if not result.ok:
            return RebuildResult(
                ingest_result=ingest_result,
                metric_results=metric_results,
                validation_summary=None,
                deleted_existing_db=deleted_existing_db,
                failure_message=result.failure_message,
            )

    validation_summary = validate_rebuild_result(event_fn=event_fn)
    failure_message = None if validation_summary.ok else "Database validation failed after rebuild."
    return RebuildResult(
        ingest_result=ingest_result,
        metric_results=metric_results,
        validation_summary=validation_summary,
        deleted_existing_db=deleted_existing_db,
        failure_message=failure_message,
    )
