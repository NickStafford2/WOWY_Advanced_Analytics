from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import (
    default_filters as _rawr_default_filters,
)
from rawr_analytics.metrics.rawr import (
    validate_filters as _validate_rawr_filters,
)
from rawr_analytics.metrics.wowy import (
    default_filters as _wowy_default_filters,
)
from rawr_analytics.metrics.wowy import (
    validate_filters as _validate_wowy_filters,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class MetricQuery:
    season_type: SeasonType
    teams: list[Team] | None
    seasons: list[Season] | None
    top_n: int
    min_average_minutes: float
    min_total_minutes: float
    min_games: int | None = None
    ridge_alpha: float | None = None
    min_games_with: int | None = None
    min_games_without: int | None = None


def build_metric_query(
    metric: Metric,
    *,
    teams: list[Team] | None = None,
    seasons: list[Season] | None = None,
    season_type: SeasonType = SeasonType.REGULAR,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games: int | None = None,
    ridge_alpha: float | None = None,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
) -> MetricQuery:
    defaults = _metric_default_filters(metric)
    normalized_teams = (
        sorted(
            {team.team_id: team for team in teams or []}.values(),
            key=lambda team: team.team_id,
        )
        or None
    )
    normalized_seasons = (
        sorted(
            {(season.start_year, season.season_type): season for season in seasons}.values(),
            key=lambda season: (season.id, season.season_type.value),
        )
        or None
    )
    normalized_season_type = season_type
    normalized_top_n = int(top_n if top_n is not None else defaults["top_n"])
    normalized_min_average_minutes = float(
        min_average_minutes if min_average_minutes is not None else defaults["min_average_minutes"]
    )
    normalized_min_total_minutes = float(
        min_total_minutes if min_total_minutes is not None else defaults["min_total_minutes"]
    )

    if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        normalized_min_games_with = int(
            min_games_with if min_games_with is not None else defaults["min_games_with"]
        )
        normalized_min_games_without = int(
            min_games_without if min_games_without is not None else defaults["min_games_without"]
        )
        _validate_wowy_filters(
            normalized_min_games_with,
            normalized_min_games_without,
            top_n=normalized_top_n,
            min_average_minutes=normalized_min_average_minutes,
            min_total_minutes=normalized_min_total_minutes,
        )
        return MetricQuery(
            season_type=normalized_season_type,
            teams=normalized_teams,
            seasons=normalized_seasons,
            top_n=normalized_top_n,
            min_average_minutes=normalized_min_average_minutes,
            min_total_minutes=normalized_min_total_minutes,
            min_games_with=normalized_min_games_with,
            min_games_without=normalized_min_games_without,
        )

    if metric == Metric.RAWR:
        normalized_min_games = int(min_games if min_games is not None else defaults["min_games"])
        normalized_ridge_alpha = float(
            ridge_alpha if ridge_alpha is not None else defaults["ridge_alpha"]
        )
        _validate_rawr_filters(
            normalized_min_games,
            ridge_alpha=normalized_ridge_alpha,
            top_n=normalized_top_n,
            min_average_minutes=normalized_min_average_minutes,
            min_total_minutes=normalized_min_total_minutes,
        )
        return MetricQuery(
            season_type=normalized_season_type,
            teams=normalized_teams,
            seasons=normalized_seasons,
            top_n=normalized_top_n,
            min_average_minutes=normalized_min_average_minutes,
            min_total_minutes=normalized_min_total_minutes,
            min_games=normalized_min_games,
            ridge_alpha=normalized_ridge_alpha,
        )

    raise ValueError(f"Unknown metric: {metric}")


def _metric_default_filters(metric: Metric) -> dict[str, int | float]:
    if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        return _wowy_default_filters()
    if metric == Metric.RAWR:
        return _rawr_default_filters()
    raise ValueError(f"Unknown metric: {metric}")
