from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.metric_store import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    RefreshScopeResult,
    prepare_metric_store_refresh,
    refresh_metric_store_scope,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import SeasonType

RefreshProgressFn = Callable[[int, int, str], None]


@dataclass(frozen=True)
class MetricStoreRefreshRequest:
    metric: Metric
    season_type: SeasonType
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA
    include_team_scopes: bool = True


@dataclass(frozen=True)
class RefreshMetricStoreResult:
    metric: Metric
    scope_results: list[RefreshScopeResult]
    warnings: list[str]
    failure_message: str | None = None

    @property
    def ok(self) -> bool:
        return self.failure_message is None

    @property
    def total_rows(self) -> int:
        return sum(scope.row_count for scope in self.scope_results)


def refresh_metric_store(
    request: MetricStoreRefreshRequest,
    *,
    progress: RefreshProgressFn | None = None,
) -> RefreshMetricStoreResult:
    plan = prepare_metric_store_refresh(
        request.metric,
        season_type=request.season_type,
        rawr_ridge_alpha=request.rawr_ridge_alpha,
        include_team_scopes=request.include_team_scopes,
    )
    if plan.failure_message is not None:
        return RefreshMetricStoreResult(
            metric=request.metric,
            scope_results=[],
            warnings=plan.warnings,
            failure_message=plan.failure_message,
        )

    scope_results: list[RefreshScopeResult] = []
    scopes = plan.scopes or []
    available_teams = plan.available_teams or []
    failure_message: str | None = None
    for index, scope in enumerate(scopes):
        if progress is not None:
            progress(index, len(scopes), f"building {scope.scope_label}")

        scope_result, should_fail_empty_rawr_scope = refresh_metric_store_scope(
            metric=request.metric,
            metric_label=plan.metric_label,
            scope=scope,
            season_type=request.season_type,
            rawr_ridge_alpha=request.rawr_ridge_alpha,
            available_teams=available_teams,
            source_fingerprint=plan.source_fingerprint,
            build_version=plan.build_version,
        )
        scope_results.append(scope_result)

        if progress is not None:
            progress(index + 1, len(scopes), f"{scope_result.status} {scope.scope_label}")

        if should_fail_empty_rawr_scope:
            failure_message = (
                "RAWR refresh produced no all-teams rows. "
                "The normalized cache is incomplete, so the web store was not updated."
            )
            break

    return RefreshMetricStoreResult(
        metric=request.metric,
        scope_results=scope_results,
        warnings=plan.warnings,
        failure_message=failure_message,
    )


__all__ = [
    "DEFAULT_RAWR_RIDGE_ALPHA",
    "MetricStoreRefreshRequest",
    "RefreshMetricStoreResult",
    "refresh_metric_store",
]
