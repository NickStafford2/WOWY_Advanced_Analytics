from __future__ import annotations

from rawr_analytics.app.metric_store import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    MetricStoreRefreshProgressEvent,
    RefreshMetricStoreResult,
    refresh_metric_store,
)
from rawr_analytics.app.rebuild._events import (
    RebuildEventFn,
    RebuildMetricRefreshProgressEvent,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import SeasonType


def refresh_rebuild_metrics(
    *,
    metrics: list[Metric],
    season_type: SeasonType,
    event_fn: RebuildEventFn | None,
) -> list[RefreshMetricStoreResult]:
    results: list[RefreshMetricStoreResult] = []
    for metric in metrics:
        results.append(
            refresh_metric_store(
                metric=metric,
                season_type=season_type,
                rawr_ridge_alpha=DEFAULT_RAWR_RIDGE_ALPHA,
                event_fn=_build_metric_refresh_event_fn(event_fn=event_fn, metric=metric),
            )
        )
        if not results[-1].ok:
            break
    return results


def default_rebuild_metrics(metrics: list[Metric] | None) -> list[Metric]:
    if metrics is not None:
        return metrics
    return [Metric.WOWY, Metric.WOWY_SHRUNK, Metric.RAWR]


def _build_metric_refresh_event_fn(
    *,
    event_fn: RebuildEventFn | None,
    metric: Metric,
):
    if event_fn is None:
        return None

    def _emit_metric_refresh_event(
        event: MetricStoreRefreshProgressEvent,
    ) -> None:
        event_fn(
            RebuildMetricRefreshProgressEvent(
                metric=metric.value,
                current=event.current,
                total=event.total,
                detail=event.detail,
            )
        )

    return _emit_metric_refresh_event
