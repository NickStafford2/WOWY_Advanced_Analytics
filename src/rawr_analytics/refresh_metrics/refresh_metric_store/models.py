from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr._calc_vars import RawrCalcVars
from rawr_analytics.metrics.wowy._calc_vars import WowyCalcVars
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

MetricStoreRefreshEventFn = Callable[["MetricStoreRefreshProgressEvent"], None]


@dataclass(frozen=True)
class RefreshCacheResult:
    metric_cache_key: str
    cache_label: str
    row_count: int
    status: str


@dataclass(frozen=True)
class MetricStoreRefreshProgressEvent:
    current: int
    total: int
    detail: str


@dataclass(frozen=True)
class RefreshMetricStoreResult:
    metric: Metric
    cache_results: list[RefreshCacheResult]
    warnings: list[str]
    failure_message: str | None = None

    @property
    def ok(self) -> bool:
        return self.failure_message is None

    @property
    def total_rows(self) -> int:
        return sum(cache.row_count for cache in self.cache_results)


@dataclass(frozen=True)
class RefreshCache:
    metric_cache_key: str
    cache_label: str
    seasons: list[Season]
    teams: list[Team]
    rawr_calc_vars: RawrCalcVars | None = None
    wowy_calc_vars: WowyCalcVars | None = None
