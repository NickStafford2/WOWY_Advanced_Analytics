from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.metric_store_query import (
    load_metric_store_scope_snapshot,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

from ._models import MetricQuery, build_metric_query


@dataclass(frozen=True)
class MetricFilters:
    teams: list[Team] | None
    seasons: list[Season] | None
    season_type: SeasonType
    min_average_minutes: float
    min_total_minutes: float
    top_n: int
    min_games: int | None = None
    ridge_alpha: float | None = None
    min_games_with: int | None = None
    min_games_without: int | None = None


@dataclass(frozen=True)
class TeamOption:
    team: Team
    label: str
    available_seasons: list[Season]


@dataclass(frozen=True)
class MetricOptionsPayload:
    metric: str
    metric_label: str
    available_teams: list[Team]
    team_options: list[TeamOption]
    available_seasons: list[Season]
    available_teams_by_season: dict[str, list[Team]]
    filters: MetricFilters


def build_metric_options_payload(
    metric: Metric,
    *,
    teams: list[Team] | None,
    season_type: SeasonType,
) -> MetricOptionsPayload:
    query = build_metric_query(metric, teams=teams, season_type=season_type)
    filters = build_filters_payload(query)
    snapshot = load_metric_store_scope_snapshot(
        metric,
        teams=teams,
        season_type=query.season_type,
    )
    return MetricOptionsPayload(
        metric=snapshot.catalog_row.metric,
        metric_label=snapshot.catalog_row.metric_label,
        available_teams=snapshot.available_teams,
        team_options=_build_team_options(
            available_teams=snapshot.available_teams,
            available_team_seasons=snapshot.available_team_seasons,
        ),
        available_seasons=snapshot.available_seasons,
        available_teams_by_season=snapshot.available_teams_by_season,
        filters=MetricFilters(
            teams=filters.teams,
            seasons=None,
            season_type=filters.season_type,
            min_average_minutes=filters.min_average_minutes,
            min_total_minutes=filters.min_total_minutes,
            top_n=filters.top_n,
            min_games=filters.min_games,
            ridge_alpha=filters.ridge_alpha,
            min_games_with=filters.min_games_with,
            min_games_without=filters.min_games_without,
        ),
    )


def build_filters_payload(query: MetricQuery) -> MetricFilters:
    return MetricFilters(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        top_n=query.top_n,
        min_games=query.min_games,
        ridge_alpha=query.ridge_alpha,
        min_games_with=query.min_games_with,
        min_games_without=query.min_games_without,
    )


def _build_team_options(
    *,
    available_teams: list[Team],
    available_team_seasons: dict[int, list[Season]],
) -> list[TeamOption]:
    return [
        TeamOption(
            team=team,
            label=team.current.abbreviation,
            available_seasons=available_team_seasons.get(team.team_id, []),
        )
        for team in sorted(available_teams, key=lambda item: item.current.abbreviation)
    ]
