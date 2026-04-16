from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.data.metric_store._mutations import prune_metric_caches
from rawr_analytics.data.metric_store.rawr import replace_rawr_metric_cache
from rawr_analytics.data.metric_store.store import load_metric_cache_store_state
from rawr_analytics.data.metric_store.usage import list_metric_cache_keys_by_usage
from rawr_analytics.data.metric_store.wowy import replace_wowy_metric_cache
from rawr_analytics.metrics._metric_cache_key import MetricCacheKey, build_rawr_metric_cache_key
from rawr_analytics.metrics._metric_cache_key import build_wowy_metric_cache_key
from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.rawr._calc_vars import RawrCalcVars, RawrEligibility
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.metrics.rawr.cache_status import list_incomplete_rawr_season_warnings
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_MIN_GAMES,
    DEFAULT_RAWR_RIDGE_ALPHA,
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    RAWR_METRIC_SUMMARY,
)
from rawr_analytics.metrics.rawr.refresh.store_rows import build_rawr_metric_store_rows
from rawr_analytics.metrics.wowy._calc_vars import WowyCalcVars, WowyEligibility
from rawr_analytics.metrics.wowy.calculate.shrinkage import DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES
from rawr_analytics.metrics.wowy.defaults import default_filters as default_wowy_filters
from rawr_analytics.metrics.wowy.defaults import describe_metric as describe_wowy_metric
from rawr_analytics.metrics.wowy.refresh.store_rows import build_wowy_metric_store_rows
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType, normalize_seasons, require_normalized_seasons
from rawr_analytics.shared.team import Team, normalize_teams

MetricStoreRefreshEventFn = Callable[["MetricStoreRefreshProgressEvent"], None]
DEFAULT_RETAINED_METRIC_CACHE_KEY_LIMIT = 10


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
class _RefreshCache:
    metric_cache_key: str
    cache_label: str
    seasons: list[Season]
    teams: list[Team]
    rawr_calc_vars: RawrCalcVars | None = None
    wowy_calc_vars: WowyCalcVars | None = None


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
        scope
        for scope in cached_team_seasons
        if scope.season.season_type == normalized_season_type
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

    caches = _build_retained_refresh_caches(
        metric=normalized_metric,
        cached_team_seasons=cached_team_seasons,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )
    metric_info = _describe_metric(normalized_metric)
    build_version = _build_metric_store_version(
        metric_info=metric_info,
    )
    warnings = (
        list_incomplete_rawr_season_warnings(
            seasons=_warning_seasons(caches),
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
        refresh_snapshot = load_game_cache_snapshot(
            teams=cache.teams,
            seasons=cache.seasons,
        )
        cache_result = _refresh_cache(
            metric=normalized_metric,
            cache=cache,
            source_fingerprint=refresh_snapshot.fingerprint,
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


def _build_all_teams_refresh_cache(
    *,
    metric: Metric,
    cached_team_seasons: list[TeamSeasonScope],
    rawr_ridge_alpha: float,
    cache_label: str,
) -> _RefreshCache:
    seasons = require_normalized_seasons([scope.season for scope in cached_team_seasons])
    teams = _available_cache_teams(cached_team_seasons)
    assert teams, "metric store refresh requires cached teams"
    rawr_calc_vars = (
        _build_refresh_rawr_calc_vars(
            seasons=seasons,
            rawr_ridge_alpha=rawr_ridge_alpha,
        )
        if metric == Metric.RAWR
        else None
    )
    wowy_calc_vars = (
        _build_refresh_wowy_calc_vars(
            metric=metric,
            seasons=seasons,
        )
        if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}
        else None
    )
    metric_cache_key = _build_refresh_cache_key(
        metric=metric,
        rawr_calc_vars=rawr_calc_vars,
        wowy_calc_vars=wowy_calc_vars,
    )
    return _RefreshCache(
        metric_cache_key=metric_cache_key,
        cache_label=cache_label,
        seasons=seasons,
        teams=teams,
        rawr_calc_vars=rawr_calc_vars,
        wowy_calc_vars=wowy_calc_vars,
    )


def _build_retained_refresh_caches(
    *,
    metric: Metric,
    cached_team_seasons: list[TeamSeasonScope],
    rawr_ridge_alpha: float,
) -> list[_RefreshCache]:
    retained_caches = _build_pinned_refresh_caches(
        metric=metric,
        cached_team_seasons=cached_team_seasons,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )
    retained_metric_cache_keys = {cache.metric_cache_key for cache in retained_caches}
    for metric_cache_key in list_metric_cache_keys_by_usage(metric_id=metric.value):
        if len(retained_caches) >= DEFAULT_RETAINED_METRIC_CACHE_KEY_LIMIT:
            break
        if metric_cache_key in retained_metric_cache_keys:
            continue
        cache = _build_refresh_cache_from_usage_key(
            metric=metric,
            metric_cache_key=metric_cache_key,
            cached_team_seasons=cached_team_seasons,
        )
        if cache is None:
            continue
        retained_caches.append(cache)
        retained_metric_cache_keys.add(cache.metric_cache_key)
    return retained_caches


def _build_pinned_refresh_caches(
    *,
    metric: Metric,
    cached_team_seasons: list[TeamSeasonScope],
    rawr_ridge_alpha: float,
) -> list[_RefreshCache]:
    retained_caches: list[_RefreshCache] = []
    regular_team_seasons = _filter_team_seasons_by_type(
        cached_team_seasons=cached_team_seasons,
        season_type=SeasonType.REGULAR,
    )
    playoff_team_seasons = _filter_team_seasons_by_type(
        cached_team_seasons=cached_team_seasons,
        season_type=SeasonType.PLAYOFFS,
    )
    combined_team_seasons = _build_combined_team_seasons(cached_team_seasons)

    for cache_label, selected_team_seasons in [
        ("all-teams regular", regular_team_seasons),
        ("all-teams playoffs", playoff_team_seasons),
        ("all-teams combined", combined_team_seasons),
    ]:
        if not selected_team_seasons:
            continue
        retained_caches.append(
            _build_all_teams_refresh_cache(
                metric=metric,
                cached_team_seasons=selected_team_seasons,
                rawr_ridge_alpha=rawr_ridge_alpha,
                cache_label=cache_label,
            )
        )
    return retained_caches


def _build_refresh_cache_from_usage_key(
    *,
    metric: Metric,
    metric_cache_key: str,
    cached_team_seasons: list[TeamSeasonScope],
) -> _RefreshCache | None:
    try:
        cache_key = MetricCacheKey.parse(metric_cache_key)
    except ValueError:
        return None
    if cache_key.metric_id != metric.value:
        return None

    seasons = _cache_seasons_for_refresh(
        cache_key=cache_key,
        cached_team_seasons=cached_team_seasons,
    )
    if seasons is None:
        return None

    teams = _cache_teams_for_refresh(
        cache_key=cache_key,
        cached_team_seasons=cached_team_seasons,
    )
    if teams is None:
        return None

    try:
        rawr_calc_vars = (
            _build_rawr_calc_vars_from_cache_key(
                cache_key=cache_key,
                seasons=seasons,
                teams=teams,
            )
            if metric == Metric.RAWR
            else None
        )
        wowy_calc_vars = (
            _build_wowy_calc_vars_from_cache_key(
                metric=metric,
                cache_key=cache_key,
                seasons=seasons,
                teams=teams,
            )
            if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}
            else None
        )
    except (KeyError, ValueError):
        return None
    canonical_metric_cache_key = _build_refresh_cache_key(
        metric=metric,
        rawr_calc_vars=rawr_calc_vars,
        wowy_calc_vars=wowy_calc_vars,
    )
    if canonical_metric_cache_key != metric_cache_key:
        return None

    return _RefreshCache(
        metric_cache_key=metric_cache_key,
        cache_label=metric_cache_key,
        seasons=seasons,
        teams=teams,
        rawr_calc_vars=rawr_calc_vars,
        wowy_calc_vars=wowy_calc_vars,
    )


def _available_cache_teams(cached_team_seasons: list[TeamSeasonScope]) -> list[Team]:
    teams = normalize_teams([scope.team for scope in cached_team_seasons])
    assert teams, "metric store refresh requires cached teams"
    return teams


def _build_refresh_cache_key(
    *,
    metric: Metric,
    rawr_calc_vars: RawrCalcVars | None = None,
    wowy_calc_vars: WowyCalcVars | None = None,
) -> str:
    if metric == Metric.RAWR:
        assert rawr_calc_vars is not None, "RAWR refresh requires calc vars"
        return build_rawr_metric_cache_key(rawr_calc_vars)
    assert wowy_calc_vars is not None, "WOWY refresh requires calc vars"
    return build_wowy_metric_cache_key(
        metric_id=metric.value,
        calc_vars=wowy_calc_vars,
    )


def _refresh_cache(
    *,
    metric: Metric,
    cache: _RefreshCache,
    source_fingerprint: str,
    build_version: str,
) -> RefreshCacheResult:
    state = load_metric_cache_store_state(metric.value, cache.metric_cache_key)
    if (
        state is not None
        and state.cache_entry_state.source_fingerprint == source_fingerprint
        and state.cache_entry_state.build_version == build_version
        and state.cache_entry_state.row_count > 0
    ):
        return RefreshCacheResult(
            metric_cache_key=cache.metric_cache_key,
            cache_label=cache.cache_label,
            row_count=state.cache_entry_state.row_count,
            status="cached",
        )

    if metric == Metric.RAWR:
        rawr_calc_vars = cache.rawr_calc_vars
        assert rawr_calc_vars is not None, "RAWR refresh requires calc vars"
        rows = build_rawr_metric_store_rows(
            calc_vars=rawr_calc_vars,
        )
        replace_rawr_metric_cache(
            metric_cache_key=cache.metric_cache_key,
            seasons=cache.seasons,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            rows=rows,
        )
    else:
        wowy_calc_vars = cache.wowy_calc_vars
        assert wowy_calc_vars is not None, "WOWY refresh requires calc vars"
        rows = build_wowy_metric_store_rows(
            metric=metric,
            calc_vars=wowy_calc_vars,
        )
        replace_wowy_metric_cache(
            metric_id=metric.value,
            metric_cache_key=cache.metric_cache_key,
            seasons=cache.seasons,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            rows=rows,
        )
    return RefreshCacheResult(
        metric_cache_key=cache.metric_cache_key,
        cache_label=cache.cache_label,
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


def _build_refresh_rawr_calc_vars(
    *,
    seasons: list[Season],
    rawr_ridge_alpha: float,
) -> RawrCalcVars:
    return RawrCalcVars(
        teams=Team.all(),
        seasons=seasons,
        eligibility=RawrEligibility(min_games=DEFAULT_RAWR_MIN_GAMES),
        ridge_alpha=rawr_ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    )


def _build_refresh_wowy_calc_vars(
    *,
    metric: Metric,
    seasons: list[Season],
) -> WowyCalcVars:
    defaults = default_wowy_filters()
    return WowyCalcVars(
        teams=Team.all(),
        seasons=seasons,
        eligibility=WowyEligibility(
            min_games_with=int(defaults["min_games_with"]),
            min_games_without=int(defaults["min_games_without"]),
        ),
        shrinkage_prior_games=(
            DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES if metric == Metric.WOWY_SHRUNK else None
        ),
    )


def _warning_seasons(caches: list[_RefreshCache]) -> list[Season]:
    seasons = normalize_seasons([season for cache in caches for season in cache.seasons])
    assert seasons is not None, "refresh warnings require non-empty retained seasons"
    return seasons


def _filter_team_seasons_by_type(
    *,
    cached_team_seasons: list[TeamSeasonScope],
    season_type: SeasonType,
) -> list[TeamSeasonScope]:
    return [
        scope for scope in cached_team_seasons if scope.season.season_type == season_type
    ]


def _build_combined_team_seasons(
    cached_team_seasons: list[TeamSeasonScope],
) -> list[TeamSeasonScope]:
    regular_years = {
        scope.season.start_year
        for scope in cached_team_seasons
        if scope.season.season_type == SeasonType.REGULAR
    }
    playoff_years = {
        scope.season.start_year
        for scope in cached_team_seasons
        if scope.season.season_type == SeasonType.PLAYOFFS
    }
    combined_years = regular_years & playoff_years
    if not combined_years:
        return []
    return [
        scope
        for scope in cached_team_seasons
        if scope.season.start_year in combined_years
        and scope.season.season_type in {SeasonType.REGULAR, SeasonType.PLAYOFFS}
    ]


def _cache_seasons_for_refresh(
    *,
    cache_key: MetricCacheKey,
    cached_team_seasons: list[TeamSeasonScope],
) -> list[Season] | None:
    seasons_by_id = {scope.season.id: scope.season for scope in cached_team_seasons}
    seasons: list[Season] = []
    for season_id in cache_key.season_ids:
        season = seasons_by_id.get(season_id)
        if season is None:
            return None
        seasons.append(season)
    return require_normalized_seasons(seasons)


def _cache_teams_for_refresh(
    *,
    cache_key: MetricCacheKey,
    cached_team_seasons: list[TeamSeasonScope],
) -> list[Team] | None:
    if not cache_key.team_ids:
        return None
    available_team_ids = {scope.team.team_id for scope in cached_team_seasons}
    if any(team_id not in available_team_ids for team_id in cache_key.team_ids):
        return None
    return [Team.from_id(team_id) for team_id in cache_key.team_ids]


def _build_rawr_calc_vars_from_cache_key(
    *,
    cache_key: MetricCacheKey,
    seasons: list[Season],
    teams: list[Team],
) -> RawrCalcVars:
    calc_settings = dict(cache_key.calc_settings)
    return RawrCalcVars(
        teams=teams,
        seasons=seasons,
        eligibility=RawrEligibility(min_games=DEFAULT_RAWR_MIN_GAMES),
        ridge_alpha=float(calc_settings["ridge_alpha"]),
        shrinkage_mode=RawrShrinkageMode.parse(calc_settings["shrinkage_mode"]),
        shrinkage_strength=float(calc_settings["shrinkage_strength"]),
        shrinkage_minute_scale=float(calc_settings["shrinkage_minute_scale"]),
    )


def _build_wowy_calc_vars_from_cache_key(
    *,
    metric: Metric,
    cache_key: MetricCacheKey,
    seasons: list[Season],
    teams: list[Team],
) -> WowyCalcVars:
    defaults = default_wowy_filters()
    calc_settings = dict(cache_key.calc_settings)
    shrinkage_prior_games = None
    if metric == Metric.WOWY_SHRUNK:
        shrinkage_prior_games = float(
            calc_settings.get(
                "shrinkage_prior_games",
                DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
            )
        )
    return WowyCalcVars(
        teams=teams,
        seasons=seasons,
        eligibility=WowyEligibility(
            min_games_with=int(defaults["min_games_with"]),
            min_games_without=int(defaults["min_games_without"]),
        ),
        shrinkage_prior_games=shrinkage_prior_games,
    )
