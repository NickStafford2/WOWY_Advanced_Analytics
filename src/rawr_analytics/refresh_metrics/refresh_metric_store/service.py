from __future__ import annotations

from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.data.metric_store._mutations import prune_metric_caches
from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.rawr.cache_status import list_incomplete_rawr_season_warnings
from rawr_analytics.metrics.rawr.defaults import DEFAULT_RAWR_RIDGE_ALPHA, RAWR_METRIC_SUMMARY
from rawr_analytics.metrics.rawr.query.service import ensure_rawr_metric_cache
from rawr_analytics.metrics.wowy.defaults import describe_metric as describe_wowy_metric
from rawr_analytics.metrics.wowy.query.service import ensure_wowy_metric_cache
from rawr_analytics.refresh_metrics.refresh_metric_store._cache_selection import (
    build_retained_refresh_caches,
    warning_seasons,
)
from rawr_analytics.refresh_metrics.refresh_metric_store.models import (
    MetricStoreRefreshEventFn,
    MetricStoreRefreshProgressEvent,
    RefreshCache,
    RefreshCacheResult,
    RefreshMetricStoreResult,
)
from rawr_analytics.shared.season import SeasonType


def refresh_metric_store(
    *,
    metric: Metric | str,
    season_type: SeasonType | str,
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
    event_fn: MetricStoreRefreshEventFn | None = None,
) -> RefreshMetricStoreResult:
    normalized_metric = Metric.parse(metric) if isinstance(metric, str) else metric
    normalized_season_type = (
        SeasonType.parse(season_type) if isinstance(season_type, str) else season_type
    )

    game_cache_snapshot = load_game_cache_snapshot()
    cached_team_seasons = game_cache_snapshot.scopes
    requested_team_seasons = [
        scope for scope in cached_team_seasons if scope.season.season_type == normalized_season_type
    ]
    if not requested_team_seasons:
        return RefreshMetricStoreResult(
            metric=normalized_metric,
            cache_results=[],
            warnings=[],
            failure_message=(
                "Normalized cache is empty for the requested season type. "
                "Rebuild ingest before refreshing the web metric store."
            ),
        )

    caches = build_retained_refresh_caches(
        metric=normalized_metric,
        cached_team_seasons=cached_team_seasons,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )
    metric_info = _describe_metric(normalized_metric)
    build_version = metric_info.build_version
    warnings = (
        list_incomplete_rawr_season_warnings(
            seasons=warning_seasons(caches),
        )
        if normalized_metric == Metric.RAWR
        else []
    )

    total_steps = len(caches) + 1
    cache_results: list[RefreshCacheResult] = []

    for index, cache in enumerate(caches, start=1):
        _emit_progress(
            event_fn,
            current=index - 1,
            total=total_steps,
            detail=f"building {cache.cache_label}",
        )
        cache_result = _refresh_cache(
            metric=normalized_metric,
            cache=cache,
            build_version=build_version,
        )
        cache_results.append(cache_result)
        _emit_progress(
            event_fn,
            current=index,
            total=total_steps,
            detail=f"{cache_result.status} {cache.cache_label}",
        )

    _emit_progress(
        event_fn,
        current=len(caches),
        total=total_steps,
        detail="pruning stale caches",
    )
    prune_metric_caches(
        metric_id=normalized_metric.value,
        retained_metric_cache_keys=[cache.metric_cache_key for cache in caches],
    )
    _emit_progress(
        event_fn,
        current=total_steps,
        total=total_steps,
        detail="pruned stale caches",
    )

    return RefreshMetricStoreResult(
        metric=normalized_metric,
        cache_results=cache_results,
        warnings=warnings,
    )


def _emit_progress(
    event_fn: MetricStoreRefreshEventFn | None,
    *,
    current: int,
    total: int,
    detail: str,
) -> None:
    if event_fn is None:
        return

    event_fn(
        MetricStoreRefreshProgressEvent(
            current=current,
            total=total,
            detail=detail,
        )
    )


def _refresh_cache(
    *,
    metric: Metric,
    cache: RefreshCache,
    build_version: str,
) -> RefreshCacheResult:
    if metric == Metric.RAWR:
        rawr_calc_vars = cache.rawr_calc_vars
        assert rawr_calc_vars is not None, "RAWR refresh requires calc vars"
        result = ensure_rawr_metric_cache(
            calc_vars=rawr_calc_vars,
            build_version=build_version,
        )
        row_count = result.row_count
        status = result.status
    else:
        wowy_calc_vars = cache.wowy_calc_vars
        assert wowy_calc_vars is not None, "WOWY refresh requires calc vars"
        result = ensure_wowy_metric_cache(
            metric=metric,
            calc_vars=wowy_calc_vars,
            build_version=build_version,
        )
        row_count = result.row_count
        status = result.status

    return RefreshCacheResult(
        metric_cache_key=cache.metric_cache_key,
        cache_label=cache.cache_label,
        row_count=row_count,
        status=status,
    )


def _describe_metric(metric: Metric) -> MetricSummary:
    if metric == Metric.RAWR:
        return RAWR_METRIC_SUMMARY
    return describe_wowy_metric(metric)
