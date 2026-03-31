from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.metric_store import DEFAULT_RAWR_RIDGE_ALPHA, RefreshMetricStoreResult
from rawr_analytics.data.metric_store import refresh_metric_store as _refresh_metric_store
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import SeasonType

RefreshProgressFn = Callable[[int, int, str], None]


@dataclass(frozen=True)
class MetricStoreRefreshRequest:
    metric: Metric
    season_type: SeasonType
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA
    include_team_scopes: bool = True


def refresh_metric_store(
    request: MetricStoreRefreshRequest,
    *,
    progress: RefreshProgressFn | None = None,
) -> RefreshMetricStoreResult:
    return _refresh_metric_store(
        request.metric,
        season_type=request.season_type,
        rawr_ridge_alpha=request.rawr_ridge_alpha,
        include_team_scopes=request.include_team_scopes,
        progress=progress,
    )


__all__ = [
    "DEFAULT_RAWR_RIDGE_ALPHA",
    "MetricStoreRefreshRequest",
    "RefreshMetricStoreResult",
    "refresh_metric_store",
]
