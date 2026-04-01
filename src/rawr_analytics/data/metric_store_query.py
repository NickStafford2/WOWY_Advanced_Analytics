from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.game_cache import (
    build_normalized_cache_fingerprint,
    list_cache_load_rows,
    list_cached_team_seasons,
)
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter
from rawr_analytics.data.player_metrics_db import (
    MetricScopeCatalogRow,
    load_metric_scope_store_state,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

__all__ = [
    "MetricStoreScopeSnapshot",
    "load_metric_store_scope_snapshot",
    "require_current_metric_scope",
]


@dataclass(frozen=True)
class MetricStoreScopeSnapshot:
    catalog_row: MetricScopeCatalogRow
    available_teams: list[Team]
    available_seasons: list[Season]
    available_team_seasons: dict[int, list[Season]]
    available_teams_by_season: dict[str, list[Team]]


def load_metric_store_scope_snapshot(
    metric: Metric,
    *,
    teams: list[Team] | None,
    season_type: SeasonType,
) -> MetricStoreScopeSnapshot:
    team_filter = build_team_filter(teams)
    scope_key = build_scope_key(season_type=season_type, team_filter=team_filter)
    catalog_row = require_current_metric_scope(metric=metric, scope_key=scope_key)
    resolved_season_type = SeasonType.parse(catalog_row.season_type)
    available_teams = [Team.from_id(team_id) for team_id in catalog_row.available_team_ids]
    available_seasons = [
        Season(season_id, resolved_season_type.to_nba_format())
        for season_id in catalog_row.available_season_ids
    ]
    return MetricStoreScopeSnapshot(
        catalog_row=catalog_row,
        available_teams=available_teams,
        available_seasons=available_seasons,
        available_team_seasons=_build_available_team_seasons(
            season_type=resolved_season_type,
            available_teams=available_teams,
            available_seasons=catalog_row.available_season_ids,
        ),
        available_teams_by_season=_build_available_teams_by_season(
            season_type=resolved_season_type,
            available_teams=available_teams,
            available_seasons=catalog_row.available_season_ids,
        ),
    )


def require_current_metric_scope(
    *,
    metric: Metric,
    scope_key: str,
) -> MetricScopeCatalogRow:
    state = load_metric_scope_store_state(metric.value, scope_key)
    if state is None:
        raise ValueError("Metric store has not been built for the requested scope")
    catalog_row = state.catalog_row
    metadata = state.metadata

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


def _build_available_team_seasons(
    *,
    season_type: SeasonType,
    available_teams: list[Team],
    available_seasons: list[str],
) -> dict[int, list[Season]]:
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
    return {
        team_id: [
            Season(season, season_type.to_nba_format())
            for season in available_seasons
            if season in seasons_by_team_id.get(team_id, set())
        ]
        for team_id in seasons_by_team_id
    }


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
