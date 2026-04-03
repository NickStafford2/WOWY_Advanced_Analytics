from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rawr_analytics.data.game_cache import (
    build_normalized_cache_fingerprint,
    list_cache_load_rows,
    list_cached_team_seasons,
)
from rawr_analytics.data.metric_store import (
    load_metric_scope_store_state,
    load_metric_span_store_rows,
)
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import RawrQuery
from rawr_analytics.metrics.wowy import WowyQuery
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

MetricQuery = RawrQuery | WowyQuery


@dataclass(frozen=True)
class MetricStoreCatalog:
    metric_label: str
    season_type: SeasonType
    available_teams: list[Team]
    available_seasons: list[Season]
    full_span_start_season_id: str | None
    full_span_end_season_id: str | None


def build_metric_scope_key(query: MetricQuery) -> str:
    return build_scope_key(
        season_type=query.season_type,
        team_filter=build_team_filter(query.teams),
    )


def build_metric_options_payload(
    *,
    metric: Metric,
    teams: list[Team] | None,
    season_type: SeasonType,
    filters: dict[str, Any],
) -> dict[str, Any]:
    catalog = require_current_metric_scope(
        metric=metric,
        scope_key=build_scope_key(
            season_type=season_type,
            team_filter=build_team_filter(teams),
        ),
    )
    return {
        "metric": metric.value,
        "metric_label": catalog.metric_label,
        "available_teams": [team.current.abbreviation for team in catalog.available_teams],
        "team_options": _build_team_options(catalog),
        "available_seasons": [season.id for season in catalog.available_seasons],
        "available_teams_by_season": _build_available_teams_by_season(catalog),
        "filters": filters,
    }


def build_metric_span_chart_payload(
    *,
    metric: Metric,
    catalog: MetricStoreCatalog,
    scope_key: str,
    top_n: int,
) -> dict[str, Any]:
    span_rows = load_metric_span_store_rows(
        metric=metric.value,
        scope_key=scope_key,
        top_n=top_n,
    )
    available_season_ids = [season.id for season in catalog.available_seasons]
    return {
        "metric": metric.value,
        "metric_label": catalog.metric_label,
        "span": {
            "start_season": catalog.full_span_start_season_id,
            "end_season": catalog.full_span_end_season_id,
            "available_seasons": available_season_ids,
            "top_n": top_n,
        },
        "series": [
            {
                "player_id": row.player_id,
                "player_name": row.player_name,
                "span_average_value": row.span_average_value,
                "season_count": row.season_count,
                "points": [
                    {
                        "season": season_id,
                        "value": span_rows.points_map.get(row.player_id, {}).get(season_id),
                    }
                    for season_id in available_season_ids
                ],
            }
            for row in span_rows.series_rows[:top_n]
        ],
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
        season_type=SeasonType.parse(catalog_row.season_type)
    )
    if state.snapshot_state.source_fingerprint != current_fingerprint:
        raise ValueError(
            "Cached metric store is stale relative to normalized cache. "
            "Refresh the web metric store after ingest is rebuilt."
        )

    season_type = SeasonType.parse(catalog_row.season_type)
    return MetricStoreCatalog(
        metric_label=catalog_row.label,
        season_type=season_type,
        available_teams=[Team.from_id(team_id) for team_id in catalog_row.available_team_ids],
        available_seasons=[
            Season(season_id, season_type.to_nba_format())
            for season_id in catalog_row.available_season_ids
        ],
        full_span_start_season_id=catalog_row.full_span_start_season_id,
        full_span_end_season_id=catalog_row.full_span_end_season_id,
    )


def selected_seasons(
    seasons: list[Season] | None,
    catalog: MetricStoreCatalog,
) -> list[str]:
    return season_ids(seasons) or [season.id for season in catalog.available_seasons]


def season_ids(seasons: list[Season] | None) -> list[str] | None:
    if seasons is None:
        return None
    return [season.id for season in seasons]


def _build_team_options(catalog: MetricStoreCatalog) -> list[dict[str, Any]]:
    seasons_by_team = _build_available_team_seasons(catalog)
    return [
        {
            "team_id": team.team_id,
            "label": team.current.abbreviation,
            "available_seasons": [
                season.id for season in seasons_by_team.get(team.team_id, [])
            ],
        }
        for team in sorted(catalog.available_teams, key=lambda item: item.current.abbreviation)
    ]


def _build_available_team_seasons(catalog: MetricStoreCatalog) -> dict[int, list[Season]]:
    available_team_ids = {team.team_id for team in catalog.available_teams}
    available_season_ids = [season.id for season in catalog.available_seasons]
    seasons_by_team_id: dict[int, set[str]] = {}
    for team_season in list_cached_team_seasons():
        if team_season.season.season_type != catalog.season_type:
            continue
        if team_season.team.team_id not in available_team_ids:
            continue
        if team_season.season.id not in available_season_ids:
            continue
        seasons_by_team_id.setdefault(team_season.team.team_id, set()).add(team_season.season.id)
    return {
        team_id: [
            Season(season_id, catalog.season_type.to_nba_format())
            for season_id in available_season_ids
            if season_id in seasons_by_team_id.get(team_id, set())
        ]
        for team_id in seasons_by_team_id
    }


def _build_available_teams_by_season(catalog: MetricStoreCatalog) -> dict[str, list[str]]:
    available_team_ids = {team.team_id for team in catalog.available_teams}
    available_season_ids = [season.id for season in catalog.available_seasons]
    teams_by_season: dict[str, set[int]] = {season_id: set() for season_id in available_season_ids}
    for team_season in list_cached_team_seasons():
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
            for team in catalog.available_teams
            if team.team_id in teams_by_season.get(season_id, set())
        ]
        for season_id in available_season_ids
    }
