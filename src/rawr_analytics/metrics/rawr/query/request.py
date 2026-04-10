from __future__ import annotations

from dataclasses import dataclass

# from rawr_analytics.metrics._query_seasons import resolve_query_seasons
from rawr_analytics.metrics.rawr.calculate.inputs import validate_filters
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_MIN_AVERAGE_MINUTES,
    DEFAULT_RAWR_MIN_GAMES,
    DEFAULT_RAWR_MIN_TOTAL_MINUTES,
    DEFAULT_RAWR_RIDGE_ALPHA,
    DEFAULT_RAWR_TOP_N,
)
from rawr_analytics.shared.season import (
    Season,
    build_all_nba_history_seasons,
    normalize_seasons,
)
from rawr_analytics.shared.team import Team, normalize_teams


@dataclass(frozen=True)
class RawrQuery:
    teams: list[Team]
    seasons: list[Season]
    top_n: int
    min_average_minutes: float
    min_total_minutes: float
    min_games: int
    ridge_alpha: float


def build_rawr_query(
    *,
    teams: list[Team] | None = None,
    seasons: list[Season] | None = None,
    # season_type: SeasonType = SeasonType.REGULAR,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games: int | None = None,
    ridge_alpha: float | None = None,
) -> RawrQuery:
    normalized_teams = normalize_teams(teams)
    normalized_seasons = normalize_seasons(seasons)
    if not normalized_seasons:
        normalized_seasons = build_all_nba_history_seasons()
    normalized_query = RawrQuery(
        # season_type=season_type,
        teams=normalized_teams,
        seasons=normalized_seasons,
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
    assert normalized_query.seasons, "RawrQuery must have a concrete non-empty season list"
    validate_filters(
        normalized_query.min_games,
        normalized_query.ridge_alpha,
        top_n=normalized_query.top_n,
        min_average_minutes=normalized_query.min_average_minutes,
        min_total_minutes=normalized_query.min_total_minutes,
    )
    return normalized_query
