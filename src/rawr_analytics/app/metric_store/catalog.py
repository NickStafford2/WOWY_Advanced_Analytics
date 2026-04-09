from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rawr_analytics.data.game_cache import (
    list_cached_scopes,
    load_cache_snapshot,
)
from rawr_analytics.data.metric_store import load_metric_scope_store_state
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class MetricCatalogAvailability:
    teams: list[Team]
    seasons: list[Season]


@dataclass(frozen=True)
class MetricSeasonSpan:
    start_season: Season
    end_season: Season


@dataclass(frozen=True)
class MetricStoreCatalog:
    season_type: SeasonType
    availability: MetricCatalogAvailability
    full_span: MetricSeasonSpan | None


def build_metric_options_payload(
    *,
    metric: Metric,
    teams: list[Team] | None,
    season_type: SeasonType,
    filters: dict[str, Any],
) -> dict[str, Any]:
    catalog = load_metric_scope_catalog_for_options(
        metric=metric,
        scope_key=build_scope_key(
            season_type=season_type,
            team_filter=build_team_filter(teams),
        ),
        teams=teams,
        season_type=season_type,
    )
    return {
        "metric": metric.value,
        "available_teams": [team.current.abbreviation for team in catalog.availability.teams],
        "team_options": _build_team_options(catalog),
        "available_seasons": [season.id for season in catalog.availability.seasons],
        "available_teams_by_season": _build_available_teams_by_season(catalog),
        "filters": filters,
    }


def require_current_metric_scope(
    *,
    metric: Metric,
    scope_key: str,
) -> MetricStoreCatalog:
    state = load_metric_scope_store_state(metric.value, scope_key)
    if state is None:
        raise ValueError("Metric store has not been built for the requested scope")

    catalog_row = state.catalog_row
    season_type = SeasonType.parse(catalog_row.season_type)
    cache_snapshot = load_cache_snapshot(season_type)
    if not cache_snapshot.entries:
        raise ValueError(
            "Normalized cache is empty for the requested scope season type. "
            "Rebuild ingest before using cached metrics."
        )

    if state.snapshot_state.source_fingerprint != cache_snapshot.fingerprint:
        raise ValueError(
            "Cached metric store is stale relative to normalized cache. "
            "Refresh the web metric store after ingest is rebuilt."
        )

    available_seasons = [
        Season.parse(season_id, season_type.to_nba_format())
        for season_id in catalog_row.available_season_ids
    ]
    return MetricStoreCatalog(
        season_type=season_type,
        availability=MetricCatalogAvailability(
            teams=[Team.from_id(team_id) for team_id in catalog_row.available_team_ids],
            seasons=available_seasons,
        ),
        full_span=_build_metric_season_span(
            season_type=season_type,
            start_season_id=catalog_row.full_span_start_season_id,
            end_season_id=catalog_row.full_span_end_season_id,
        ),
    )


def load_metric_scope_catalog_for_options(
    *,
    metric: Metric,
    scope_key: str,
    teams: list[Team] | None,
    season_type: SeasonType,
) -> MetricStoreCatalog:
    try:
        return require_current_metric_scope(metric=metric, scope_key=scope_key)
    except ValueError:
        return _build_metric_options_catalog_from_cache(
            teams=teams,
            season_type=season_type,
        )


def _build_metric_options_catalog_from_cache(
    *,
    teams: list[Team] | None,
    season_type: SeasonType,
) -> MetricStoreCatalog:
    cached_team_seasons = [
        team_season
        for team_season in list_cached_scopes(teams=teams)
        if team_season.season.season_type == season_type
    ]
    if not cached_team_seasons:
        raise ValueError(
            "Normalized cache is empty for the requested scope season type. "
            "Rebuild ingest before using cached metrics."
        )

    available_team_ids = sorted({team_season.team.team_id for team_season in cached_team_seasons})
    available_season_ids = sorted({team_season.season.id for team_season in cached_team_seasons})
    return MetricStoreCatalog(
        season_type=season_type,
        availability=MetricCatalogAvailability(
            teams=[Team.from_id(team_id) for team_id in available_team_ids],
            seasons=[
                Season.parse(season_id, season_type.to_nba_format())
                for season_id in available_season_ids
            ],
        ),
        full_span=_build_metric_season_span(
            season_type=season_type,
            start_season_id=available_season_ids[0],
            end_season_id=available_season_ids[-1],
        ),
    )


def _build_team_options(catalog: MetricStoreCatalog) -> list[dict[str, Any]]:
    seasons_by_team = _build_available_team_seasons(catalog)
    return [
        {
            "team_id": team.team_id,
            "label": team.current.abbreviation,
            "available_seasons": [season.id for season in seasons_by_team.get(team.team_id, [])],
        }
        for team in sorted(catalog.availability.teams, key=lambda item: item.current.abbreviation)
    ]


def _build_available_team_seasons(catalog: MetricStoreCatalog) -> dict[int, list[Season]]:
    available_team_ids = {team.team_id for team in catalog.availability.teams}
    available_season_ids = [season.id for season in catalog.availability.seasons]
    seasons_by_team_id: dict[int, set[str]] = {}
    for team_season in list_cached_scopes():
        if team_season.season.season_type != catalog.season_type:
            continue
        if team_season.team.team_id not in available_team_ids:
            continue
        if team_season.season.id not in available_season_ids:
            continue
        seasons_by_team_id.setdefault(team_season.team.team_id, set()).add(team_season.season.id)
    return {
        team_id: [
            Season.parse(season_id, catalog.season_type.to_nba_format())
            for season_id in available_season_ids
            if season_id in seasons_by_team_id.get(team_id, set())
        ]
        for team_id in seasons_by_team_id
    }


def _build_available_teams_by_season(catalog: MetricStoreCatalog) -> dict[str, list[str]]:
    available_team_ids = {team.team_id for team in catalog.availability.teams}
    available_season_ids = [season.id for season in catalog.availability.seasons]
    teams_by_season: dict[str, set[int]] = {season_id: set() for season_id in available_season_ids}
    for team_season in list_cached_scopes():
        if team_season.season.season_type != catalog.season_type:
            continue
        if team_season.season.id not in teams_by_season:
            continue
        if team_season.team.team_id not in available_team_ids:
            continue
        teams_by_season[team_season.season.id].add(team_season.team.team_id)
    return {
        season_id: [
            team.current.abbreviation
            for team in catalog.availability.teams
            if team.team_id in teams_by_season.get(season_id, set())
        ]
        for season_id in available_season_ids
    }


def _build_metric_season_span(
    *,
    season_type: SeasonType,
    start_season_id: str | None,
    end_season_id: str | None,
) -> MetricSeasonSpan | None:
    if start_season_id is None:
        return None
    assert end_season_id is not None, "metric store full-span seasons must be paired"
    return MetricSeasonSpan(
        start_season=Season.parse(start_season_id, season_type.to_nba_format()),
        end_season=Season.parse(end_season_id, season_type.to_nba_format()),
    )
