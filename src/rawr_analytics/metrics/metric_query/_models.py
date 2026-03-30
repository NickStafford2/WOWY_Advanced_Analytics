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


@dataclass(frozen=True)
class MetricQuery:
    season_type: SeasonType
    team_ids: list[int] | None
    seasons: list[str] | None
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
    team_ids: list[int] | None = None,
    seasons: list[str] | None = None,
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
    normalized_team_ids = sorted({team_id for team_id in team_ids or [] if team_id > 0}) or None
    normalized_seasons = (
        [Season(season, season_type.to_nba_format()).id for season in seasons] if seasons else None
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
            team_ids=normalized_team_ids,
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
            team_ids=normalized_team_ids,
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
