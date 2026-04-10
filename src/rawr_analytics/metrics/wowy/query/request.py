from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics._player_context import PlayerSeasonFilters
from rawr_analytics.metrics.wowy.calculate.inputs import WowyEligibility, validate_filters
from rawr_analytics.metrics.wowy.defaults import default_filters
from rawr_analytics.shared.season import (
    Season,
    build_all_nba_history_seasons,
    normalize_seasons,
)
from rawr_analytics.shared.team import Team, normalize_teams


@dataclass(frozen=True)
class WowyQuery:
    teams: list[Team]
    seasons: list[Season]
    top_n: int
    eligibility: WowyEligibility
    filters: PlayerSeasonFilters


def build_wowy_query(
    *,
    teams: list[Team] | None = None,
    seasons: list[Season] | None = None,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
) -> WowyQuery:
    defaults = default_filters()
    normalized_teams = normalize_teams(teams)
    if not normalized_teams:
        normalized_teams = Team.all()
    normalized_seasons = normalize_seasons(seasons)
    if not normalized_seasons:
        normalized_seasons = build_all_nba_history_seasons()
    normalized_query = WowyQuery(
        teams=normalized_teams,
        seasons=normalized_seasons,
        top_n=int(top_n if top_n is not None else defaults["top_n"]),
        eligibility=WowyEligibility(
            min_games_with=int(
                min_games_with if min_games_with is not None else defaults["min_games_with"]
            ),
            min_games_without=int(
                min_games_without
                if min_games_without is not None
                else defaults["min_games_without"]
            ),
        ),
        filters=PlayerSeasonFilters(
            min_average_minutes=float(
                min_average_minutes
                if min_average_minutes is not None
                else defaults["min_average_minutes"]
            ),
            min_total_minutes=float(
                min_total_minutes
                if min_total_minutes is not None
                else defaults["min_total_minutes"]
            ),
        ),
    )
    assert normalized_query.seasons, "WowyQuery must have a concrete non-empty season list"
    validate_filters(
        normalized_query.eligibility.min_games_with,
        normalized_query.eligibility.min_games_without,
        top_n=normalized_query.top_n,
        min_average_minutes=normalized_query.filters.min_average_minutes,
        min_total_minutes=normalized_query.filters.min_total_minutes,
    )
    return normalized_query
