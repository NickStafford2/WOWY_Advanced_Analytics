from __future__ import annotations

from rawr_analytics.data.metric_store.usage import list_metric_cache_keys_by_usage
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.refresh_metrics.refresh_metric_store._cache_keys import (
    WOWY_REFRESH_METRICS,
    build_refresh_cache_from_usage_key,
    build_refresh_cache_key,
    build_refresh_rawr_calc_vars,
    build_refresh_wowy_calc_vars,
)
from rawr_analytics.refresh_metrics.refresh_metric_store.models import RefreshCache
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import (
    Season,
    SeasonType,
    normalize_seasons,
    require_normalized_seasons,
)
from rawr_analytics.shared.team import Team, normalize_teams

DEFAULT_RETAINED_METRIC_CACHE_KEY_LIMIT = 10


def build_retained_refresh_caches(
    *,
    metric: Metric,
    cached_team_seasons: list[TeamSeasonScope],
    rawr_ridge_alpha: float,
) -> list[RefreshCache]:
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

        cache = build_refresh_cache_from_usage_key(
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
) -> list[RefreshCache]:
    retained_caches: list[RefreshCache] = []

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


def _build_all_teams_refresh_cache(
    *,
    metric: Metric,
    cached_team_seasons: list[TeamSeasonScope],
    rawr_ridge_alpha: float,
    cache_label: str,
) -> RefreshCache:
    seasons = require_normalized_seasons([scope.season for scope in cached_team_seasons])
    teams = _available_cache_teams(cached_team_seasons)

    rawr_calc_vars = (
        build_refresh_rawr_calc_vars(
            seasons=seasons,
            rawr_ridge_alpha=rawr_ridge_alpha,
        )
        if metric == Metric.RAWR
        else None
    )
    wowy_calc_vars = (
        build_refresh_wowy_calc_vars(
            metric=metric,
            seasons=seasons,
        )
        if metric in WOWY_REFRESH_METRICS
        else None
    )
    metric_cache_key = build_refresh_cache_key(
        metric=metric,
        rawr_calc_vars=rawr_calc_vars,
        wowy_calc_vars=wowy_calc_vars,
    )

    return RefreshCache(
        metric_cache_key=metric_cache_key,
        cache_label=cache_label,
        seasons=seasons,
        teams=teams,
        rawr_calc_vars=rawr_calc_vars,
        wowy_calc_vars=wowy_calc_vars,
    )


def _available_cache_teams(cached_team_seasons: list[TeamSeasonScope]) -> list[Team]:
    teams = normalize_teams([scope.team for scope in cached_team_seasons])
    assert teams, "metric store refresh requires cached teams"
    return teams


def warning_seasons(caches: list[RefreshCache]) -> list[Season]:
    seasons = normalize_seasons([season for cache in caches for season in cache.seasons])
    assert seasons is not None, "refresh warnings require non-empty retained seasons"
    return seasons


def _filter_team_seasons_by_type(
    *,
    cached_team_seasons: list[TeamSeasonScope],
    season_type: SeasonType,
) -> list[TeamSeasonScope]:
    return [scope for scope in cached_team_seasons if scope.season.season_type == season_type]


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
