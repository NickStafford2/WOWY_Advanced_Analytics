from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

from .models import MetricQuery


@dataclass(frozen=True)
class RawrMetricFilters:
    teams: list[Team] | None
    seasons: list[Season] | None
    season_type: SeasonType
    min_average_minutes: float
    min_total_minutes: float
    top_n: int
    min_games: int
    ridge_alpha: float


@dataclass(frozen=True)
class WowyMetricFilters:
    teams: list[Team] | None
    seasons: list[Season] | None
    season_type: SeasonType
    min_average_minutes: float
    min_total_minutes: float
    top_n: int
    min_games_with: int
    min_games_without: int


type MetricFilters = RawrMetricFilters | WowyMetricFilters


@dataclass(frozen=True)
class TeamOption:
    team: Team
    label: str
    available_seasons: list[Season]


@dataclass(frozen=True)
class MetricStoreCatalog:
    metric: str
    metric_label: str
    season_type: SeasonType
    available_teams: list[Team]
    available_seasons: list[Season]
    full_span_start_season_id: str | None
    full_span_end_season_id: str | None


@dataclass(frozen=True)
class MetricOptionsPayload:
    metric: str
    metric_label: str
    available_teams: list[Team]
    team_options: list[TeamOption]
    available_seasons: list[Season]
    available_teams_by_season: dict[str, list[Team]]
    filters: MetricFilters


@dataclass(frozen=True)
class MetricStoreScopeSnapshot:
    catalog: MetricStoreCatalog
    available_team_seasons: dict[int, list[Season]]
    available_teams_by_season: dict[str, list[Team]]


def build_metric_options_payload(
    snapshot: MetricStoreScopeSnapshot,
    *,
    filters: MetricFilters,
) -> MetricOptionsPayload:
    return MetricOptionsPayload(
        metric=snapshot.catalog.metric,
        metric_label=snapshot.catalog.metric_label,
        available_teams=snapshot.catalog.available_teams,
        team_options=_build_team_options(
            available_teams=snapshot.catalog.available_teams,
            available_team_seasons=snapshot.available_team_seasons,
        ),
        available_seasons=snapshot.catalog.available_seasons,
        available_teams_by_season=snapshot.available_teams_by_season,
        filters=_build_options_filters(filters),
    )


def build_filters_payload(metric: Metric, query: MetricQuery) -> MetricFilters:
    if metric == Metric.RAWR:
        assert query.min_games is not None
        assert query.ridge_alpha is not None
        return RawrMetricFilters(
            teams=query.teams,
            seasons=query.seasons,
            season_type=query.season_type,
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            top_n=query.top_n,
            min_games=query.min_games,
            ridge_alpha=query.ridge_alpha,
        )
    if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        assert query.min_games_with is not None
        assert query.min_games_without is not None
        return WowyMetricFilters(
            teams=query.teams,
            seasons=query.seasons,
            season_type=query.season_type,
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            top_n=query.top_n,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )
    raise ValueError(f"Unknown metric: {metric}")


def _build_options_filters(filters: MetricFilters) -> MetricFilters:
    if isinstance(filters, RawrMetricFilters):
        return RawrMetricFilters(
            teams=filters.teams,
            seasons=None,
            season_type=filters.season_type,
            min_average_minutes=filters.min_average_minutes,
            min_total_minutes=filters.min_total_minutes,
            top_n=filters.top_n,
            min_games=filters.min_games,
            ridge_alpha=filters.ridge_alpha,
        )
    return WowyMetricFilters(
        teams=filters.teams,
        seasons=None,
        season_type=filters.season_type,
        min_average_minutes=filters.min_average_minutes,
        min_total_minutes=filters.min_total_minutes,
        top_n=filters.top_n,
        min_games_with=filters.min_games_with,
        min_games_without=filters.min_games_without,
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
