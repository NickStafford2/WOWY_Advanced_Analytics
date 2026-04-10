from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.app._query_seasons import resolve_query_seasons
from rawr_analytics.metrics.wowy.defaults import default_filters
from rawr_analytics.metrics.wowy.inputs import validate_filters
from rawr_analytics.shared.season import Season, SeasonType, normalize_seasons
from rawr_analytics.shared.team import Team, normalize_teams


@dataclass(frozen=True)
class WowyQuery:
    season_type: SeasonType
    teams: list[Team] | None
    seasons: list[Season]
    top_n: int
    min_average_minutes: float
    min_total_minutes: float
    min_games_with: int
    min_games_without: int


def build_wowy_query(
    *,
    teams: list[Team] | None = None,
    seasons: list[Season] | None = None,
    season_type: SeasonType = SeasonType.REGULAR,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
) -> WowyQuery:
    defaults = default_filters()
    normalized_teams = normalize_teams(teams)
    normalized_season_filter = normalize_seasons(seasons)
    normalized_query = WowyQuery(
        season_type=season_type,
        teams=normalized_teams,
        seasons=resolve_query_seasons(
            teams=normalized_teams,
            season_filter=normalized_season_filter,
            season_type=season_type,
        ),
        top_n=int(top_n if top_n is not None else defaults["top_n"]),
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
        min_games_with=int(
            min_games_with if min_games_with is not None else defaults["min_games_with"]
        ),
        min_games_without=int(
            min_games_without
            if min_games_without is not None
            else defaults["min_games_without"]
        ),
    )
    assert normalized_query.seasons, "WowyQuery must have a concrete non-empty season list"
    validate_filters(
        normalized_query.min_games_with,
        normalized_query.min_games_without,
        top_n=normalized_query.top_n,
        min_average_minutes=normalized_query.min_average_minutes,
        min_total_minutes=normalized_query.min_total_minutes,
    )
    return normalized_query
