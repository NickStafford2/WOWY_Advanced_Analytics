from __future__ import annotations

from collections.abc import Sequence
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
from rawr_analytics.metrics.rawr import RawrQuery, RawrQueryFilters
from rawr_analytics.metrics.wowy import WowyQuery, WowyQueryFilters
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

MetricView = str


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
class MetricStoreScopeSnapshot:
    catalog: MetricStoreCatalog
    available_team_seasons: dict[int, list[Season]]
    available_teams_by_season: dict[str, list[Team]]


@dataclass(frozen=True)
class MetricOptionsPayload:
    metric: str
    metric_label: str
    available_teams: list[Team]
    team_options: list[TeamOption]
    available_seasons: list[Season]
    available_teams_by_season: dict[str, list[Team]]
    filters: RawrQueryFilters | WowyQueryFilters


@dataclass(frozen=True)
class MetricViewResult:
    metric: Metric
    view: MetricView
    query: RawrQuery | WowyQuery
    payload: dict[str, Any]


@dataclass(frozen=True)
class MetricExportResult:
    metric: Metric
    view: MetricView
    query: RawrQuery | WowyQuery
    metric_label: str
    rows: list[dict[str, Any]]


def build_options_payload(
    snapshot: MetricStoreScopeSnapshot,
    *,
    filters: RawrQueryFilters | WowyQueryFilters,
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
        filters=filters,
    )


def build_metric_store_scope_snapshot(
    metric: Metric,
    *,
    teams: list[Team] | None,
    season_type: SeasonType,
) -> MetricStoreScopeSnapshot:
    catalog = require_current_metric_scope(
        metric=metric,
        scope_key=build_scope_key(
            season_type=season_type,
            team_filter=build_team_filter(teams),
        ),
    )
    return MetricStoreScopeSnapshot(
        catalog=catalog,
        available_team_seasons=_build_available_team_seasons(
            season_type=catalog.season_type,
            available_teams=catalog.available_teams,
            available_season_ids=[season.id for season in catalog.available_seasons],
        ),
        available_teams_by_season=_build_available_teams_by_season(
            season_type=catalog.season_type,
            available_teams=catalog.available_teams,
            available_season_ids=[season.id for season in catalog.available_seasons],
        ),
    )


def build_metric_scope_key(query: RawrQuery | WowyQuery) -> str:
    return build_scope_key(
        season_type=query.season_type,
        team_filter=build_team_filter(query.teams),
    )


def build_span_chart_payload(
    *,
    metric: Metric,
    catalog: MetricStoreCatalog,
    top_n: int,
) -> dict[str, Any]:
    span_rows = load_metric_span_store_rows(
        metric=metric.value,
        scope_key=build_scope_key(
            season_type=catalog.season_type,
            team_filter=build_team_filter(catalog.available_teams),
        ),
        top_n=top_n,
    )
    return _build_span_chart_series_payload(
        metric=metric,
        catalog=catalog,
        series_rows=span_rows.series_rows,
        points_map=span_rows.points_map,
        top_n=top_n,
    )


def build_span_chart_payload_for_scope(
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
    return _build_span_chart_series_payload(
        metric=metric,
        catalog=catalog,
        series_rows=span_rows.series_rows,
        points_map=span_rows.points_map,
        top_n=top_n,
    )


def require_current_metric_scope(
    *,
    metric: Metric,
    scope_key: str,
) -> MetricStoreCatalog:
    state = load_metric_scope_store_state(metric.value, scope_key)
    if state is None:
        raise ValueError("Metric store has not been built for the requested scope")

    catalog_row = state.catalog_row
    snapshot_state = state.snapshot_state
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
    if snapshot_state.source_fingerprint != current_fingerprint:
        raise ValueError(
            "Cached metric store is stale relative to normalized cache. "
            "Refresh the web metric store after ingest is rebuilt."
        )

    resolved_season_type = SeasonType.parse(catalog_row.season_type)
    return MetricStoreCatalog(
        metric=catalog_row.metric_id,
        metric_label=catalog_row.label,
        season_type=resolved_season_type,
        available_teams=[Team.from_id(team_id) for team_id in catalog_row.available_team_ids],
        available_seasons=[
            Season(season_id, resolved_season_type.to_nba_format())
            for season_id in catalog_row.available_season_ids
        ],
        full_span_start_season_id=catalog_row.full_span_start_season_id,
        full_span_end_season_id=catalog_row.full_span_end_season_id,
    )


def require_wowy_metric(metric: Metric) -> None:
    if metric not in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        raise ValueError(f"Unknown WOWY metric: {metric}")


def season_ids(seasons: list[Season] | None) -> list[str] | None:
    if seasons is None:
        return None
    return [season.id for season in seasons]


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


def _build_span_chart_series_payload(
    *,
    metric: Metric,
    catalog: MetricStoreCatalog,
    series_rows: Sequence[Any],
    points_map: dict[int, dict[str, float]],
    top_n: int,
) -> dict[str, Any]:
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
                        "value": points_map.get(row.player_id, {}).get(season_id),
                    }
                    for season_id in available_season_ids
                ],
            }
            for row in series_rows[:top_n]
        ],
    }


def _build_available_team_seasons(
    *,
    season_type: SeasonType,
    available_teams: list[Team],
    available_season_ids: list[str],
) -> dict[int, list[Season]]:
    available_team_ids = {team.team_id for team in available_teams}
    available_season_set = set(available_season_ids)
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
            Season(season_id, season_type.to_nba_format())
            for season_id in available_season_ids
            if season_id in seasons_by_team_id.get(team_id, set())
        ]
        for team_id in seasons_by_team_id
    }


def _build_available_teams_by_season(
    *,
    season_type: SeasonType,
    available_teams: list[Team],
    available_season_ids: list[str],
) -> dict[str, list[Team]]:
    available_team_ids = {team.team_id for team in available_teams}
    available_season_set = set(available_season_ids)
    teams_by_season: dict[str, set[int]] = {season_id: set() for season_id in available_season_ids}
    for team_season in list_cached_team_seasons():
        if team_season.season.season_type != season_type:
            continue
        if team_season.season.id not in available_season_set:
            continue
        if team_season.team.team_id not in available_team_ids:
            continue
        teams_by_season.setdefault(team_season.season.id, set()).add(team_season.team.team_id)
    return {
        season_id: [
            team
            for team in available_teams
            if team.team_id in teams_by_season.get(season_id, set())
        ]
        for season_id in available_season_ids
    }


__all__ = [
    "MetricExportResult",
    "MetricOptionsPayload",
    "MetricStoreCatalog",
    "MetricStoreScopeSnapshot",
    "MetricView",
    "MetricViewResult",
    "TeamOption",
    "build_metric_scope_key",
    "build_metric_store_scope_snapshot",
    "build_options_payload",
    "build_span_chart_payload_for_scope",
    "require_current_metric_scope",
    "require_wowy_metric",
    "season_ids",
]
