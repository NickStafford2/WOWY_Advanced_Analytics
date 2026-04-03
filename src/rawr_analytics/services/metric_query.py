from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, cast

from rawr_analytics.data.game_cache import (
    build_normalized_cache_fingerprint,
    list_cache_load_rows,
    list_cached_team_seasons,
)
from rawr_analytics.data.metric_store import (
    load_metric_scope_store_state,
    load_metric_span_store_rows,
    load_rawr_player_season_value_rows,
    load_wowy_player_season_value_rows,
)
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import (
    RawrQuery,
    RawrQueryFilters,
    build_rawr_custom_query,
)
from rawr_analytics.metrics.rawr import (
    build_cached_leaderboard_payload as _build_rawr_cached_leaderboard_payload,
)
from rawr_analytics.metrics.rawr import (
    build_custom_leaderboard_payload as _build_rawr_custom_leaderboard_payload,
)
from rawr_analytics.metrics.rawr import (
    build_export_table as _build_rawr_export_table,
)
from rawr_analytics.metrics.rawr import (
    build_options_filters_payload as _build_rawr_options_filters_payload,
)
from rawr_analytics.metrics.rawr import (
    build_player_seasons_payload as _build_rawr_player_seasons_payload,
)
from rawr_analytics.metrics.rawr import (
    build_query_filters_payload as _build_rawr_query_filters_payload,
)
from rawr_analytics.metrics.wowy import (
    WowyQuery,
    WowyQueryFilters,
    build_wowy_custom_query,
)
from rawr_analytics.metrics.wowy import (
    build_cached_leaderboard_payload as _build_wowy_cached_leaderboard_payload,
)
from rawr_analytics.metrics.wowy import (
    build_custom_leaderboard_payload as _build_wowy_custom_leaderboard_payload,
)
from rawr_analytics.metrics.wowy import (
    build_export_table as _build_wowy_export_table,
)
from rawr_analytics.metrics.wowy import (
    build_options_filters_payload as _build_wowy_options_filters_payload,
)
from rawr_analytics.metrics.wowy import (
    build_player_seasons_payload as _build_wowy_player_seasons_payload,
)
from rawr_analytics.metrics.wowy import (
    build_query_filters_payload as _build_wowy_query_filters_payload,
)
from rawr_analytics.services._metric_inputs import (
    load_rawr_season_inputs,
    load_wowy_season_inputs,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

MetricQuery = RawrQuery | WowyQuery
MetricView = str
MetricQueryExport = tuple[str, list[dict[str, Any]]]


@dataclass(frozen=True)
class _MetricStoreCatalog:
    metric_label: str
    season_type: SeasonType
    available_teams: list[Team]
    available_seasons: list[Season]
    full_span_start_season_id: str | None
    full_span_end_season_id: str | None


def build_rawr_options_payload(query: RawrQuery) -> dict[str, Any]:
    filters = _build_rawr_options_filters_payload(_build_rawr_filters_payload(query))
    return _build_options_payload(
        metric=Metric.RAWR,
        teams=query.teams,
        season_type=query.season_type,
        filters=filters,
    )


def build_wowy_options_payload(
    metric: Metric,
    query: WowyQuery,
) -> dict[str, Any]:
    _require_wowy_metric(metric)
    filters = _build_wowy_options_filters_payload(_build_wowy_filters_payload(query))
    return _build_options_payload(
        metric=metric,
        teams=query.teams,
        season_type=query.season_type,
        filters=filters,
    )


def build_rawr_query_view(
    query: RawrQuery,
    *,
    view: MetricView,
) -> dict[str, Any]:
    payload = _build_rawr_view_payload(view=view, query=query)
    payload["filters"] = _serialize_service_value(_build_rawr_filters_payload(query))
    return payload


def build_wowy_query_view(
    metric: Metric,
    query: WowyQuery,
    *,
    view: MetricView,
) -> dict[str, Any]:
    _require_wowy_metric(metric)
    payload = _build_wowy_view_payload(metric, view=view, query=query)
    payload["filters"] = _serialize_service_value(_build_wowy_filters_payload(query))
    return payload


def build_rawr_query_export(
    query: RawrQuery,
    *,
    view: MetricView,
) -> MetricQueryExport:
    return _build_rawr_export_payload(view=view, query=query)


def build_wowy_query_export(
    metric: Metric,
    query: WowyQuery,
    view: MetricView,
) -> MetricQueryExport:
    _require_wowy_metric(metric)
    return _build_wowy_export_payload(metric, view=view, query=query)


def _build_options_payload(
    *,
    metric: Metric,
    teams: list[Team] | None,
    season_type: SeasonType,
    filters: RawrQueryFilters | WowyQueryFilters,
) -> dict[str, Any]:
    catalog = _require_current_metric_scope(
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
        "filters": _serialize_service_value(filters),
    }


def _build_rawr_view_payload(
    *,
    view: MetricView,
    query: RawrQuery,
) -> dict[str, Any]:
    scope_key = _build_scope_key(query)

    if view == "player-seasons":
        _require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        rows = load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=_season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
        return _build_rawr_player_seasons_payload(cast(Sequence[Any], rows))

    if view == "cached-leaderboard":
        catalog = _require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        rows = load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=_season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
        payload = _build_rawr_cached_leaderboard_payload(
            metric_label=catalog.metric_label,
            available_seasons=catalog.available_seasons,
            available_teams=catalog.available_teams,
            rows=cast(Sequence[Any], rows),
            seasons=_selected_seasons(query.seasons, catalog),
            top_n=query.top_n,
        )
        return _serialize_service_value(payload)

    if view == "span-chart":
        catalog = _require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        return _build_span_chart_payload(
            metric=Metric.RAWR,
            catalog=catalog,
            scope_key=scope_key,
            top_n=query.top_n,
        )

    if view == "custom-query":
        result = _build_rawr_custom_query_result(query)
        return _build_rawr_custom_leaderboard_payload(result, top_n=query.top_n)

    raise ValueError(f"Unknown metric view: {view}")


def _build_wowy_view_payload(
    metric: Metric,
    *,
    view: MetricView,
    query: WowyQuery,
) -> dict[str, Any]:
    scope_key = _build_scope_key(query)

    if view == "player-seasons":
        _require_current_metric_scope(metric=metric, scope_key=scope_key)
        rows = load_wowy_player_season_value_rows(
            metric_id=metric.value,
            scope_key=scope_key,
            seasons=_season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )
        return _build_wowy_player_seasons_payload(metric, cast(Sequence[Any], rows))

    if view == "cached-leaderboard":
        catalog = _require_current_metric_scope(metric=metric, scope_key=scope_key)
        rows = load_wowy_player_season_value_rows(
            metric_id=metric.value,
            scope_key=scope_key,
            seasons=_season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )
        payload = _build_wowy_cached_leaderboard_payload(
            metric,
            metric_label=catalog.metric_label,
            available_seasons=catalog.available_seasons,
            available_teams=catalog.available_teams,
            rows=cast(Sequence[Any], rows),
            seasons=_selected_seasons(query.seasons, catalog),
            top_n=query.top_n,
        )
        return _serialize_service_value(payload)

    if view == "span-chart":
        catalog = _require_current_metric_scope(metric=metric, scope_key=scope_key)
        return _build_span_chart_payload(
            metric=metric,
            catalog=catalog,
            scope_key=scope_key,
            top_n=query.top_n,
        )

    if view == "custom-query":
        result = _build_wowy_custom_query_result(metric, query)
        return _build_wowy_custom_leaderboard_payload(metric, result, top_n=query.top_n)

    raise ValueError(f"Unknown metric view: {view}")


def _build_rawr_export_payload(
    *,
    view: MetricView,
    query: RawrQuery,
) -> MetricQueryExport:
    scope_key = _build_scope_key(query)

    if view == "cached-leaderboard":
        catalog = _require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        rows = load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=_season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
        return _build_rawr_export_table(
            rows=cast(Sequence[Any], rows),
            seasons=_selected_seasons(query.seasons, catalog),
        )

    if view == "custom-query":
        result = _build_rawr_custom_query_result(query)
        return _build_rawr_export_table(
            rows=result.rows,
            seasons=sorted({row.season_id for row in result.rows}),
            metric_label=result.metric_label,
        )

    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_wowy_export_payload(
    metric: Metric,
    *,
    view: MetricView,
    query: WowyQuery,
) -> MetricQueryExport:
    scope_key = _build_scope_key(query)

    if view == "cached-leaderboard":
        catalog = _require_current_metric_scope(metric=metric, scope_key=scope_key)
        rows = load_wowy_player_season_value_rows(
            metric_id=metric.value,
            scope_key=scope_key,
            seasons=_season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )
        return _build_wowy_export_table(
            metric,
            rows=cast(Sequence[Any], rows),
            seasons=_selected_seasons(query.seasons, catalog),
        )

    if view == "custom-query":
        result = _build_wowy_custom_query_result(metric, query)
        return _build_wowy_export_table(
            metric,
            rows=result.rows,
            seasons=sorted({row.season_id for row in result.rows}),
            metric_label=result.metric_label,
        )

    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_rawr_filters_payload(query: RawrQuery) -> RawrQueryFilters:
    return _build_rawr_query_filters_payload(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        top_n=query.top_n,
        min_games=query.min_games,
        ridge_alpha=query.ridge_alpha,
    )


def _build_wowy_filters_payload(query: WowyQuery) -> WowyQueryFilters:
    return _build_wowy_query_filters_payload(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        top_n=query.top_n,
        min_games_with=query.min_games_with,
        min_games_without=query.min_games_without,
    )


def _build_rawr_custom_query_result(query: RawrQuery):
    season_inputs = load_rawr_season_inputs(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
    )
    return build_rawr_custom_query(
        season_inputs=season_inputs,
        min_games=query.min_games,
        ridge_alpha=query.ridge_alpha,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
    )


def _build_wowy_custom_query_result(metric: Metric, query: WowyQuery):
    season_inputs = load_wowy_season_inputs(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
    )
    return build_wowy_custom_query(
        metric,
        season_inputs=season_inputs,
        min_games_with=query.min_games_with,
        min_games_without=query.min_games_without,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
    )


def _build_span_chart_payload(
    *,
    metric: Metric,
    catalog: _MetricStoreCatalog,
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


def _require_current_metric_scope(
    *,
    metric: Metric,
    scope_key: str,
) -> _MetricStoreCatalog:
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
    return _MetricStoreCatalog(
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


def _build_scope_key(query: MetricQuery) -> str:
    return build_scope_key(
        season_type=query.season_type,
        team_filter=build_team_filter(query.teams),
    )


def _selected_seasons(
    seasons: list[Season] | None,
    catalog: _MetricStoreCatalog,
) -> list[str]:
    return _season_ids(seasons) or [season.id for season in catalog.available_seasons]


def _season_ids(seasons: list[Season] | None) -> list[str] | None:
    if seasons is None:
        return None
    return [season.id for season in seasons]


def _build_team_options(catalog: _MetricStoreCatalog) -> list[dict[str, Any]]:
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


def _build_available_team_seasons(catalog: _MetricStoreCatalog) -> dict[int, list[Season]]:
    available_team_ids = {team.team_id for team in catalog.available_teams}
    available_season_ids = [season.id for season in catalog.available_seasons]
    available_season_set = set(available_season_ids)
    seasons_by_team_id: dict[int, set[str]] = {}
    for team_season in list_cached_team_seasons():
        if team_season.season.season_type != catalog.season_type:
            continue
        if team_season.team.team_id not in available_team_ids:
            continue
        if team_season.season.id not in available_season_set:
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


def _build_available_teams_by_season(catalog: _MetricStoreCatalog) -> dict[str, list[str]]:
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


def _serialize_service_value(value: Any) -> Any:
    if isinstance(value, Team):
        return value.current.abbreviation
    if isinstance(value, Season):
        return value.id
    if isinstance(value, SeasonType):
        return value.to_nba_format()
    if isinstance(value, Metric):
        return value.value
    if isinstance(value, RawrQueryFilters):
        return {
            "team": (
                None
                if value.teams is None
                else [team.current.abbreviation for team in value.teams]
            ),
            "team_id": None if value.teams is None else [team.team_id for team in value.teams],
            "season": None if value.seasons is None else [season.id for season in value.seasons],
            "season_type": value.season_type.to_nba_format(),
            "min_average_minutes": value.min_average_minutes,
            "min_total_minutes": value.min_total_minutes,
            "top_n": value.top_n,
            "min_games": value.min_games,
            "ridge_alpha": value.ridge_alpha,
        }
    if isinstance(value, WowyQueryFilters):
        return {
            "team": (
                None
                if value.teams is None
                else [team.current.abbreviation for team in value.teams]
            ),
            "team_id": None if value.teams is None else [team.team_id for team in value.teams],
            "season": None if value.seasons is None else [season.id for season in value.seasons],
            "season_type": value.season_type.to_nba_format(),
            "min_average_minutes": value.min_average_minutes,
            "min_total_minutes": value.min_total_minutes,
            "top_n": value.top_n,
            "min_games_with": value.min_games_with,
            "min_games_without": value.min_games_without,
        }
    if isinstance(value, dict):
        return {key: _serialize_service_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_serialize_service_value(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    return value


def _require_wowy_metric(metric: Metric) -> None:
    if metric not in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        raise ValueError(f"Unknown WOWY metric: {metric}")


__all__ = [
    "build_rawr_options_payload",
    "build_rawr_query_export",
    "build_rawr_query_view",
    "build_wowy_options_payload",
    "build_wowy_query_export",
    "build_wowy_query_view",
]
