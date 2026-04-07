from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics._query_normalization import (
    normalize_query_seasons,
    normalize_query_teams,
)
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_MIN_AVERAGE_MINUTES,
    DEFAULT_RAWR_MIN_GAMES,
    DEFAULT_RAWR_MIN_TOTAL_MINUTES,
    DEFAULT_RAWR_RIDGE_ALPHA,
    DEFAULT_RAWR_TOP_N,
)
from rawr_analytics.metrics.rawr.inputs import validate_filters
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrQuery:
    season_type: SeasonType
    teams: list[Team] | None
    seasons: list[Season] | None
    top_n: int
    min_average_minutes: float
    min_total_minutes: float
    min_games: int
    ridge_alpha: float


def build_rawr_query(
    *,
    teams: list[Team] | None = None,
    seasons: list[Season] | None = None,
    season_type: SeasonType = SeasonType.REGULAR,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games: int | None = None,
    ridge_alpha: float | None = None,
) -> RawrQuery:
    normalized_query = RawrQuery(
        season_type=season_type,
        teams=normalize_query_teams(teams),
        seasons=normalize_query_seasons(seasons),
        top_n=int(top_n if top_n is not None else DEFAULT_RAWR_TOP_N),
        min_average_minutes=float(
            min_average_minutes
            if min_average_minutes is not None
            else DEFAULT_RAWR_MIN_AVERAGE_MINUTES
        ),
        min_total_minutes=float(
            min_total_minutes if min_total_minutes is not None else DEFAULT_RAWR_MIN_TOTAL_MINUTES
        ),
        min_games=int(min_games if min_games is not None else DEFAULT_RAWR_MIN_GAMES),
        ridge_alpha=float(ridge_alpha if ridge_alpha is not None else DEFAULT_RAWR_RIDGE_ALPHA),
    )
    validate_filters(
        normalized_query.min_games,
        normalized_query.ridge_alpha,
        top_n=normalized_query.top_n,
        min_average_minutes=normalized_query.min_average_minutes,
        min_total_minutes=normalized_query.min_total_minutes,
    )
    return normalized_query
