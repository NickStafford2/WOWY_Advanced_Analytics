from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rawr_analytics.metrics._query_normalization import (
    normalize_query_seasons,
    normalize_query_teams,
)
from rawr_analytics.metrics.rawr.defaults import DEFAULT_RAWR_FILTERS
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

    def without_seasons(self) -> RawrQuery:
        return RawrQuery(
            season_type=self.season_type,
            teams=self.teams,
            seasons=None,
            top_n=self.top_n,
            min_average_minutes=self.min_average_minutes,
            min_total_minutes=self.min_total_minutes,
            min_games=self.min_games,
            ridge_alpha=self.ridge_alpha,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "team": (
                None if self.teams is None else [team.current.abbreviation for team in self.teams]
            ),
            "team_id": None if self.teams is None else [team.team_id for team in self.teams],
            "season": None if self.seasons is None else [season.id for season in self.seasons],
            "season_type": self.season_type.to_nba_format(),
            "min_average_minutes": self.min_average_minutes,
            "min_total_minutes": self.min_total_minutes,
            "top_n": self.top_n,
            "min_games": self.min_games,
            "ridge_alpha": self.ridge_alpha,
        }


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
        top_n=int(top_n if top_n is not None else DEFAULT_RAWR_FILTERS.top_n),
        min_average_minutes=float(
            min_average_minutes
            if min_average_minutes is not None
            else DEFAULT_RAWR_FILTERS.min_average_minutes
        ),
        min_total_minutes=float(
            min_total_minutes
            if min_total_minutes is not None
            else DEFAULT_RAWR_FILTERS.min_total_minutes
        ),
        min_games=int(min_games if min_games is not None else DEFAULT_RAWR_FILTERS.min_games),
        ridge_alpha=float(
            ridge_alpha if ridge_alpha is not None else DEFAULT_RAWR_FILTERS.ridge_alpha
        ),
    )
    validate_filters(
        normalized_query.min_games,
        normalized_query.ridge_alpha,
        top_n=normalized_query.top_n,
        min_average_minutes=normalized_query.min_average_minutes,
        min_total_minutes=normalized_query.min_total_minutes,
    )
    return normalized_query
