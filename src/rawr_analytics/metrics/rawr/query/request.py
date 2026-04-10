from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics._player_context import PlayerSeasonFilters
from rawr_analytics.metrics.rawr.calculate.inputs import RawrEligibility, validate_filters
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
    eligibility: RawrEligibility
    filters: PlayerSeasonFilters
    ridge_alpha: float


def build_rawr_query(
    *,
    teams: list[Team] | None = None,
    seasons: list[Season] | None = None,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games: int | None = None,
    ridge_alpha: float | None = None,
) -> RawrQuery:
    normalized_teams = normalize_teams(teams)
    if not normalized_teams:
        normalized_teams = Team.all()
    normalized_seasons = normalize_seasons(seasons)
    if not normalized_seasons:
        normalized_seasons = build_all_nba_history_seasons()
    normalized_query = RawrQuery(
        teams=normalized_teams,
        seasons=normalized_seasons,
        top_n=int(top_n if top_n is not None else DEFAULT_RAWR_TOP_N),
        eligibility=RawrEligibility(
            min_games=int(min_games if min_games is not None else DEFAULT_RAWR_MIN_GAMES),
        ),
        filters=PlayerSeasonFilters(
            min_average_minutes=float(
                min_average_minutes
                if min_average_minutes is not None
                else DEFAULT_RAWR_MIN_AVERAGE_MINUTES
            ),
            min_total_minutes=float(
                min_total_minutes
                if min_total_minutes is not None
                else DEFAULT_RAWR_MIN_TOTAL_MINUTES
            ),
        ),
        ridge_alpha=float(ridge_alpha if ridge_alpha is not None else DEFAULT_RAWR_RIDGE_ALPHA),
    )
    assert normalized_query.seasons, "RawrQuery must have a concrete non-empty season list"
    validate_filters(
        normalized_query.eligibility.min_games,
        normalized_query.ridge_alpha,
        top_n=normalized_query.top_n,
        min_average_minutes=normalized_query.filters.min_average_minutes,
        min_total_minutes=normalized_query.filters.min_total_minutes,
    )
    return normalized_query
