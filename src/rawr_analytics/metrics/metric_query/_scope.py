from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.game_cache import (
    build_normalized_cache_fingerprint,
    list_cache_load_rows,
    list_cached_team_seasons,
)
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter
from rawr_analytics.data.player_metrics_db.models import MetricScopeCatalogRow
from rawr_analytics.data.player_metrics_db.queries import (
    load_metric_scope_catalog_row,
    load_metric_store_metadata,
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
    filters = _build_filters_payload(query)
    team_filter = build_team_filter(teams)
    scope_key = build_scope_key(season_type=query.season_type, team_filter=team_filter)
    catalog_row = _require_current_metric_scope(metric=metric, scope_key=scope_key)
    available_teams = [Team.from_id(team_id) for team_id in catalog_row.available_team_ids]
    available_seasons = [
        Season(season_id, SeasonType.parse(catalog_row.season_type).to_nba_format())
        for season_id in catalog_row.available_seasons
    ]
    return MetricOptionsPayload(
        metric=catalog_row.metric,
        metric_label=catalog_row.metric_label,
        available_teams=available_teams,
        team_options=_build_team_options(
            season_type=SeasonType.parse(catalog_row.season_type),
            available_teams=available_teams,
            available_seasons=catalog_row.available_seasons,
        ),
        available_seasons=available_seasons,
        available_teams_by_season=_build_available_teams_by_season(
            season_type=SeasonType.parse(catalog_row.season_type),
            available_teams=available_teams,
            available_seasons=catalog_row.available_seasons,
        ),
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


def _build_filters_payload(query: MetricQuery) -> MetricFilters:
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
    season_type: SeasonType,
    available_teams: list[Team],
    available_seasons: list[str],
) -> list[TeamOption]:
    available_team_ids = {team.team_id for team in available_teams}
    available_season_set = set(available_seasons)
    seasons_by_team_id: dict[int, set[str]] = {}
    for team_season in list_cached_team_seasons():
        if team_season.season.season_type != season_type:
            continue
        if team_season.team.team_id not in available_team_ids:
            continue
        if team_season.season.id not in available_season_set:
            continue
        seasons_by_team_id.setdefault(team_season.team.team_id, set()).add(team_season.season.id)
    return [
        TeamOption(
            team=Team.from_id(team_id),
            label=Team.from_id(team_id).current.abbreviation,
            available_seasons=[
                Season(season, season_type.to_nba_format())
                for season in available_seasons
                if season in seasons_by_team_id[team_id]
            ],
        )
        for team_id in sorted(
            seasons_by_team_id,
            key=lambda item: Team.from_id(item).current.abbreviation,
        )
    ]


def _build_available_teams_by_season(
    *,
    season_type: SeasonType,
    available_teams: list[Team],
    available_seasons: list[str],
) -> dict[str, list[Team]]:
    available_team_ids = {team.team_id for team in available_teams}
    available_season_set = set(available_seasons)
    teams_by_season: dict[str, set[int]] = {season: set() for season in available_seasons}
    for team_season in list_cached_team_seasons():
        if team_season.season.season_type != season_type:
            continue
        if team_season.season.id not in available_season_set:
            continue
        if team_season.team.team_id not in available_team_ids:
            continue
        teams_by_season.setdefault(team_season.season.id, set()).add(team_season.team.team_id)
    return {
        season: [
            team for team in available_teams if team.team_id in teams_by_season.get(season, set())
        ]
        for season in available_seasons
    }


def _require_current_metric_scope(
    *,
    metric: Metric,
    scope_key: str,
) -> MetricScopeCatalogRow:
    catalog_row = load_metric_scope_catalog_row(metric.value, scope_key)
    if catalog_row is None:
        raise ValueError("Metric store has not been built for the requested scope")

    metadata = load_metric_store_metadata(metric.value, scope_key)
    if metadata is None:
        raise ValueError("Metric store metadata is missing for the requested scope")

    cache_load_rows = [
        row
        for row in list_cache_load_rows()
        if row.season.season_type.to_nba_format() == catalog_row.season_type
    ]
    if not cache_load_rows:
        raise ValueError(
            "Normalized cache is empty for the requested scope season type. "
            "Rebuild ingest before using cached metrics."
        )

    current_fingerprint = build_normalized_cache_fingerprint(
        season=Season("2000", catalog_row.season_type)
    )
    if metadata.source_fingerprint != current_fingerprint:
        raise ValueError(
            "Cached metric store is stale relative to normalized cache. "
            "Refresh the web metric store after ingest is rebuilt."
        )
    return catalog_row
