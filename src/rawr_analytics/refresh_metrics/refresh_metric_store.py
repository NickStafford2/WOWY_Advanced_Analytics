from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.data.metric_store.rawr import replace_rawr_scope_snapshot
from rawr_analytics.data.metric_store.store import load_metric_scope_store_state
from rawr_analytics.data.metric_store.wowy import replace_wowy_scope_snapshot
from rawr_analytics.data.metric_store_scope import build_team_filter
from rawr_analytics.metrics._metric_cache_key import (
    build_rawr_metric_cache_key,
    build_wowy_metric_cache_key,
)
from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.rawr.calculate.inputs import RawrEligibility
from rawr_analytics.metrics.rawr.cache_status import list_incomplete_rawr_season_warnings
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_MIN_GAMES,
    DEFAULT_RAWR_RIDGE_ALPHA,
    RAWR_METRIC_SUMMARY,
)
from rawr_analytics.metrics.rawr.query.request import RawrCalcVars
from rawr_analytics.metrics.rawr.refresh.store_rows import build_rawr_metric_store_rows
from rawr_analytics.metrics.wowy.defaults import describe_metric as describe_wowy_metric
from rawr_analytics.metrics.wowy.defaults import default_filters as default_wowy_filters
from rawr_analytics.metrics.wowy.calculate.inputs import WowyEligibility
from rawr_analytics.metrics.wowy.query.request import WowyCalcVars
from rawr_analytics.metrics.wowy.refresh.store_rows import build_wowy_metric_store_rows
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType, require_normalized_seasons
from rawr_analytics.shared.team import Team, normalize_teams

MetricStoreRefreshEventFn = Callable[["MetricStoreRefreshProgressEvent"], None]


@dataclass(frozen=True)
class RefreshScopeResult:
    metric_cache_key: str
    scope_label: str
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
    scope_results: list[RefreshScopeResult]
    warnings: list[str]
    failure_message: str | None = None

    @property
    def ok(self) -> bool:
        return self.failure_message is None

    @property
    def total_rows(self) -> int:
        return sum(scope.row_count for scope in self.scope_results)


@dataclass(frozen=True)
class _RefreshScope:
    metric_cache_key: str
    scope_label: str
    team_filter: str
    seasons: list[Season]
    teams: list[Team]


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
    cached_team_seasons = [
        scope
        for scope in game_cache_snapshot.scopes
        if scope.season.season_type == normalized_season_type
    ]
    if not cached_team_seasons:
        return RefreshMetricStoreResult(
            metric=normalized_metric,
            scope_results=[],
            warnings=[],
            failure_message=(
                "Normalized cache is empty for the requested season type. "
                "Rebuild ingest before refreshing the web metric store."
            ),
        )

    scope = _build_all_teams_refresh_scope(
        metric=normalized_metric,
        cached_team_seasons=cached_team_seasons,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )
    available_teams = _available_cache_teams(cached_team_seasons)
    metric_info = _describe_metric(normalized_metric)
    build_version = _build_metric_store_version(
        metric_info=metric_info,
    )
    warnings = (
        list_incomplete_rawr_season_warnings(seasons=scope.seasons)
        if normalized_metric == Metric.RAWR
        else []
    )

    _emit_progress(
        event_fn,
        current=0,
        total=1,
        detail=f"building {scope.scope_label}",
    )
    scope_result = _refresh_scope(
        metric=normalized_metric,
        scope=scope,
        season_type=normalized_season_type,
        available_teams=available_teams,
        source_fingerprint=game_cache_snapshot.fingerprint,
        build_version=build_version,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )
    _emit_progress(
        event_fn,
        current=1,
        total=1,
        detail=f"{scope_result.status} {scope.scope_label}",
    )
    return RefreshMetricStoreResult(
        metric=normalized_metric,
        scope_results=[scope_result],
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


def _build_all_teams_refresh_scope(
    *,
    metric: Metric,
    cached_team_seasons: list[TeamSeasonScope],
    rawr_ridge_alpha: float,
) -> _RefreshScope:
    seasons = require_normalized_seasons([scope.season for scope in cached_team_seasons])
    teams = _available_cache_teams(cached_team_seasons)
    assert teams, "metric store refresh requires cached teams"
    team_filter = build_team_filter(None)
    metric_cache_key = _build_refresh_cache_key(
        metric=metric,
        seasons=seasons,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )
    return _RefreshScope(
        metric_cache_key=metric_cache_key,
        scope_label="all-teams",
        team_filter=team_filter,
        seasons=seasons,
        teams=teams,
    )


def _available_cache_teams(cached_team_seasons: list[TeamSeasonScope]) -> list[Team]:
    teams = normalize_teams([scope.team for scope in cached_team_seasons])
    assert teams, "metric store refresh requires cached teams"
    return teams


def _build_refresh_cache_key(
    *,
    metric: Metric,
    seasons: list[Season],
    rawr_ridge_alpha: float,
) -> str:
    all_teams = Team.all()
    if metric == Metric.RAWR:
        return build_rawr_metric_cache_key(
            RawrCalcVars(
                teams=all_teams,
                seasons=seasons,
                eligibility=RawrEligibility(min_games=DEFAULT_RAWR_MIN_GAMES),
                ridge_alpha=rawr_ridge_alpha,
            )
        )
    defaults = default_wowy_filters()
    return build_wowy_metric_cache_key(
        metric_id=metric.value,
        calc_vars=WowyCalcVars(
            teams=all_teams,
            seasons=seasons,
            eligibility=WowyEligibility(
                min_games_with=int(defaults["min_games_with"]),
                min_games_without=int(defaults["min_games_without"]),
            ),
        ),
    )


def _refresh_scope(
    *,
    metric: Metric,
    scope: _RefreshScope,
    season_type: SeasonType,
    available_teams: list[Team],
    source_fingerprint: str,
    build_version: str,
    rawr_ridge_alpha: float,
) -> RefreshScopeResult:
    state = load_metric_scope_store_state(metric.value, scope.metric_cache_key)
    if (
        state is not None
        and state.snapshot_state.source_fingerprint == source_fingerprint
        and state.snapshot_state.build_version == build_version
        and state.snapshot_state.row_count > 0
    ):
        return RefreshScopeResult(
            metric_cache_key=scope.metric_cache_key,
            scope_label=scope.scope_label,
            row_count=state.snapshot_state.row_count,
            status="cached",
        )

    if metric == Metric.RAWR:
        rows = build_rawr_metric_store_rows(
            seasons=scope.seasons,
            teams=scope.teams,
            rawr_ridge_alpha=rawr_ridge_alpha,
        )
        replace_rawr_scope_snapshot(
            metric_cache_key=scope.metric_cache_key,
            label=metric.value,
            team_filter=scope.team_filter,
            season_type=season_type,
            seasons=scope.seasons,
            available_teams=available_teams,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            rows=rows,
        )
    else:
        rows = build_wowy_metric_store_rows(
            metric=metric,
            seasons=scope.seasons,
            teams=scope.teams,
        )
        replace_wowy_scope_snapshot(
            metric_id=metric.value,
            metric_cache_key=scope.metric_cache_key,
            label=metric.value,
            team_filter=scope.team_filter,
            season_type=season_type,
            seasons=scope.seasons,
            available_teams=available_teams,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            rows=rows,
        )
    return RefreshScopeResult(
        metric_cache_key=scope.metric_cache_key,
        scope_label=scope.scope_label,
        row_count=len(rows),
        status="built",
    )


def _build_metric_store_version(
    *,
    metric_info: MetricSummary,
) -> str:
    return metric_info.build_version


def _describe_metric(metric: Metric) -> MetricSummary:
    if metric == Metric.RAWR:
        return RAWR_METRIC_SUMMARY
    return describe_wowy_metric(metric)
