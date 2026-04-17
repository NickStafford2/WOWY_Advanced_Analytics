from __future__ import annotations

from rawr_analytics.metrics._metric_cache_key import (
    MetricCacheKey,
    build_rawr_metric_cache_key,
    build_wowy_metric_cache_key,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr._calc_vars import RawrParams, RawrEligibility
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.metrics.rawr.defaults import DEFAULT_RAWR_RIDGE_ALPHA
from rawr_analytics.metrics.wowy._calc_vars import WowyParams, WowyEligibility
from rawr_analytics.metrics.wowy.calculate.shrinkage import DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES
from rawr_analytics.metrics.wowy.defaults import default_filters as default_wowy_filters
from rawr_analytics.refresh_metrics.refresh_metric_store.models import RefreshCache
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, require_normalized_seasons
from rawr_analytics.shared.team import Team

RAWR_REFRESH_METRICS = {Metric.RAWR}
WOWY_REFRESH_METRICS = {Metric.WOWY, Metric.WOWY_SHRUNK}


def build_refresh_cache_key(
    *,
    metric: Metric,
    rawr_calc_vars: RawrParams | None = None,
    wowy_calc_vars: WowyParams | None = None,
) -> str:
    if metric == Metric.RAWR:
        assert rawr_calc_vars is not None, "RAWR refresh requires calc vars"
        return build_rawr_metric_cache_key(rawr_calc_vars)

    assert wowy_calc_vars is not None, "WOWY refresh requires calc vars"
    return build_wowy_metric_cache_key(
        metric_id=metric.value,
        calc_vars=wowy_calc_vars,
    )


def build_refresh_rawr_calc_vars(
    *,
    seasons: list[Season],
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
) -> RawrParams:
    return RawrParams(
        teams=Team.all(),
        seasons=seasons,
        eligibility=RawrEligibility(),
        ridge_alpha=rawr_ridge_alpha,
    )


def build_refresh_wowy_calc_vars(
    *,
    metric: Metric,
    seasons: list[Season],
) -> WowyParams:
    defaults = default_wowy_filters()
    return WowyParams(
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


def build_refresh_cache_from_usage_key(
    *,
    metric: Metric,
    metric_cache_key: str,
    cached_team_seasons: list[TeamSeasonScope],
) -> RefreshCache | None:
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
            if metric in RAWR_REFRESH_METRICS
            else None
        )
        wowy_calc_vars = (
            _build_wowy_calc_vars_from_cache_key(
                metric=metric,
                cache_key=cache_key,
                seasons=seasons,
                teams=teams,
            )
            if metric in WOWY_REFRESH_METRICS
            else None
        )
    except (KeyError, ValueError):
        return None

    canonical_metric_cache_key = build_refresh_cache_key(
        metric=metric,
        rawr_calc_vars=rawr_calc_vars,
        wowy_calc_vars=wowy_calc_vars,
    )
    if canonical_metric_cache_key != metric_cache_key:
        return None

    return RefreshCache(
        metric_cache_key=metric_cache_key,
        cache_label=metric_cache_key,
        seasons=seasons,
        teams=teams,
        rawr_calc_vars=rawr_calc_vars,
        wowy_calc_vars=wowy_calc_vars,
    )


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
) -> RawrParams:
    calc_settings = dict(cache_key.calc_settings)
    return RawrParams(
        teams=teams,
        seasons=seasons,
        eligibility=RawrEligibility(),
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
) -> WowyParams:
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

    return WowyParams(
        teams=teams,
        seasons=seasons,
        eligibility=WowyEligibility(
            min_games_with=int(defaults["min_games_with"]),
            min_games_without=int(defaults["min_games_without"]),
        ),
        shrinkage_prior_games=shrinkage_prior_games,
    )
