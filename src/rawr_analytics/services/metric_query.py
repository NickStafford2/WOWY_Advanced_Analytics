from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from typing import Any

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
from rawr_analytics.metrics.metric_query import (
    MetricOptionsPayload,
    MetricQuery,
    MetricStoreCatalog,
    MetricStoreScopeSnapshot,
    RawrMetricFilters,
    TeamOption,
    WowyMetricFilters,
)
from rawr_analytics.metrics.metric_query import (
    build_filters_payload as _build_filters_payload,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_cached_leaderboard_payload as _build_metric_cached_leaderboard_payload,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_custom_leaderboard_payload as _build_metric_custom_leaderboard_payload,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_export_table as _build_metric_export_table,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_options_payload as _build_metric_options_payload,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_player_seasons_payload as _build_metric_player_seasons_payload,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_query as _build_metric_query,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_span_chart_payload as _build_metric_span_chart_payload,
)
from rawr_analytics.metrics.rawr import build_rawr_custom_query
from rawr_analytics.metrics.rawr import default_filters as _rawr_default_filters
from rawr_analytics.metrics.wowy import build_wowy_custom_query
from rawr_analytics.services._metric_inputs import (
    load_rawr_season_inputs,
    load_wowy_season_inputs,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

MetricView = str


@dataclass(frozen=True)
class MetricQueryRequest:
    metric: Metric
    season_type: SeasonType
    teams: list[Team] | None = None
    seasons: list[Season] | None = None
    top_n: int | None = None
    min_average_minutes: float | None = None
    min_total_minutes: float | None = None
    min_games: int | None = None
    ridge_alpha: float | None = None
    min_games_with: int | None = None
    min_games_without: int | None = None


@dataclass(frozen=True)
class MetricViewResult:
    metric: Metric
    view: MetricView
    query: MetricQuery
    payload: dict[str, Any]


@dataclass(frozen=True)
class MetricExportResult:
    metric: Metric
    view: MetricView
    query: MetricQuery
    metric_label: str
    rows: list[dict[str, Any]]


def serialize_service_value(value: Any) -> Any:
    if isinstance(value, Team):
        return value.current.abbreviation
    if isinstance(value, Season):
        return value.id
    if isinstance(value, SeasonType):
        return value.to_nba_format()
    if isinstance(value, Metric):
        return value.value
    if isinstance(value, RawrMetricFilters):
        return _serialize_rawr_metric_filters(value)
    if isinstance(value, WowyMetricFilters):
        return _serialize_wowy_metric_filters(value)
    if isinstance(value, TeamOption):
        return {
            "team_id": value.team.team_id,
            "label": value.label,
            "available_seasons": [season.id for season in value.available_seasons],
        }
    if isinstance(value, MetricOptionsPayload):
        return {
            "metric": value.metric,
            "metric_label": value.metric_label,
            "available_teams": [team.current.abbreviation for team in value.available_teams],
            "team_options": [serialize_service_value(option) for option in value.team_options],
            "available_seasons": [season.id for season in value.available_seasons],
            "available_teams_by_season": {
                season_id: [team.current.abbreviation for team in teams]
                for season_id, teams in value.available_teams_by_season.items()
            },
            "filters": serialize_service_value(value.filters),
        }
    if is_dataclass(value):
        return {
            field.name: serialize_service_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, dict):
        return {key: serialize_service_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [serialize_service_value(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    return value


def build_metric_options_payload(request: MetricQueryRequest) -> MetricOptionsPayload:
    query = _build_query(request)
    filters = _build_filters_payload(request.metric, query)
    snapshot = _load_metric_store_scope_snapshot(
        request.metric,
        teams=query.teams,
        season_type=query.season_type,
    )
    return _build_metric_options_payload(
        snapshot,
        filters=filters,
    )


def build_metric_query_view(
    request: MetricQueryRequest,
    *,
    view: MetricView,
) -> MetricViewResult:
    query = _build_query(request)
    payload = _build_view_payload(request.metric, view=view, query=query)
    payload["filters"] = _build_filters_payload(request.metric, query)
    return MetricViewResult(
        metric=request.metric,
        view=view,
        query=query,
        payload=payload,
    )


def build_metric_query_export(
    request: MetricQueryRequest,
    *,
    view: MetricView,
) -> MetricExportResult:
    query = _build_query(request)
    metric_label, rows = _build_export_table(request.metric, view=view, query=query)
    return MetricExportResult(
        metric=request.metric,
        view=view,
        query=query,
        metric_label=metric_label,
        rows=rows,
    )


def _build_query(request: MetricQueryRequest) -> MetricQuery:
    return _build_metric_query(
        request.metric,
        teams=request.teams,
        seasons=request.seasons,
        season_type=request.season_type,
        top_n=request.top_n,
        min_average_minutes=request.min_average_minutes,
        min_total_minutes=request.min_total_minutes,
        min_games=request.min_games,
        ridge_alpha=request.ridge_alpha,
        min_games_with=request.min_games_with,
        min_games_without=request.min_games_without,
    )


def _build_view_payload(
    metric: Metric,
    *,
    view: MetricView,
    query: MetricQuery,
) -> dict[str, Any]:
    scope_key = _build_scope_key(query)

    if view == "player-seasons":
        if metric == Metric.RAWR:
            _require_current_metric_scope(metric=metric, scope_key=scope_key)
            rows = load_rawr_player_season_value_rows(
                scope_key=scope_key,
                seasons=_season_ids(query.seasons),
                min_average_minutes=query.min_average_minutes,
                min_total_minutes=query.min_total_minutes,
                min_games=query.min_games,
            )
        elif metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
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
        else:
            raise ValueError(f"Unknown metric: {metric}")
        return _build_metric_player_seasons_payload(metric, rows=rows)

    if view == "cached-leaderboard":
        catalog = _require_current_metric_scope(metric=metric, scope_key=scope_key)
        selected_seasons = _season_ids(query.seasons) or [
            season.id for season in catalog.available_seasons
        ]
        if metric == Metric.RAWR:
            rows = load_rawr_player_season_value_rows(
                scope_key=scope_key,
                seasons=_season_ids(query.seasons),
                min_average_minutes=query.min_average_minutes,
                min_total_minutes=query.min_total_minutes,
                min_games=query.min_games,
            )
        elif metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
            rows = load_wowy_player_season_value_rows(
                metric_id=metric.value,
                scope_key=scope_key,
                seasons=_season_ids(query.seasons),
                min_average_minutes=query.min_average_minutes,
                min_total_minutes=query.min_total_minutes,
                min_games_with=query.min_games_with,
                min_games_without=query.min_games_without,
            )
        else:
            raise ValueError(f"Unknown metric: {metric}")
        return _build_metric_cached_leaderboard_payload(
            metric,
            catalog=catalog,
            rows=rows,
            seasons=selected_seasons,
            top_n=query.top_n,
        )

    if view == "span-chart":
        catalog = _require_current_metric_scope(metric=metric, scope_key=scope_key)
        span_rows = load_metric_span_store_rows(
            metric=metric.value,
            scope_key=scope_key,
            top_n=query.top_n,
        )
        return _build_metric_span_chart_payload(
            metric,
            catalog=catalog,
            series_rows=span_rows.series_rows,
            points_map=span_rows.points_map,
            top_n=query.top_n,
        )

    if view == "custom-query":
        result = _build_custom_query_result(metric, query=query)
        return _build_metric_custom_leaderboard_payload(
            metric,
            result=result,
            top_n=query.top_n,
        )

    raise ValueError(f"Unknown metric view: {view}")


def _build_export_table(
    metric: Metric,
    *,
    view: MetricView,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    scope_key = _build_scope_key(query)

    if view == "cached-leaderboard":
        catalog = _require_current_metric_scope(metric=metric, scope_key=scope_key)
        seasons = _season_ids(query.seasons) or [season.id for season in catalog.available_seasons]
        if metric == Metric.RAWR:
            rows = load_rawr_player_season_value_rows(
                scope_key=scope_key,
                seasons=_season_ids(query.seasons),
                min_average_minutes=query.min_average_minutes,
                min_total_minutes=query.min_total_minutes,
                min_games=query.min_games,
            )
        elif metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
            rows = load_wowy_player_season_value_rows(
                metric_id=metric.value,
                scope_key=scope_key,
                seasons=_season_ids(query.seasons),
                min_average_minutes=query.min_average_minutes,
                min_total_minutes=query.min_total_minutes,
                min_games_with=query.min_games_with,
                min_games_without=query.min_games_without,
            )
        else:
            raise ValueError(f"Unknown metric: {metric}")
        return _build_metric_export_table(metric, rows=rows, seasons=seasons)

    if view == "custom-query":
        result = _build_custom_query_result(metric, query=query)
        return _build_metric_export_table(
            metric,
            rows=result.rows,
            seasons=sorted({row.season_id for row in result.rows}),
            metric_label=result.metric_label,
        )

    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_custom_query_result(
    metric: Metric,
    *,
    query: MetricQuery,
):
    if metric == Metric.RAWR:
        season_inputs = load_rawr_season_inputs(
            teams=query.teams,
            seasons=query.seasons,
            season_type=query.season_type,
        )
        return build_rawr_custom_query(
            season_inputs=season_inputs,
            min_games=int(query.min_games or 0),
            ridge_alpha=float(query.ridge_alpha or _rawr_default_filters()["ridge_alpha"]),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
        )
    if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        season_inputs = load_wowy_season_inputs(
            teams=query.teams,
            seasons=query.seasons,
            season_type=query.season_type,
        )
        return build_wowy_custom_query(
            metric,
            season_inputs=season_inputs,
            min_games_with=int(query.min_games_with or 0),
            min_games_without=int(query.min_games_without or 0),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
        )
    raise ValueError(f"Unknown metric: {metric}")


def _load_metric_store_scope_snapshot(
    metric: Metric,
    *,
    teams: list[Team] | None,
    season_type: SeasonType,
) -> MetricStoreScopeSnapshot:
    catalog = _require_current_metric_scope(
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


def _require_current_metric_scope(
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


def _build_scope_key(query: MetricQuery) -> str:
    return build_scope_key(
        season_type=query.season_type,
        team_filter=build_team_filter(query.teams),
    )


def _season_ids(seasons: list[Season] | None) -> list[str] | None:
    if seasons is None:
        return None
    return [season.id for season in seasons]


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


def _parse_optional_int(raw_value: str | None) -> int | None:
    return None if raw_value is None else int(raw_value)


def _parse_optional_float(raw_value: str | None) -> float | None:
    return None if raw_value is None else float(raw_value)


def _parse_positive_int_list(raw_values: list[str]) -> list[int] | None:
    if not raw_values:
        return None
    parsed_values: list[int] = []
    for raw_value in raw_values:
        value = int(raw_value)
        if value <= 0:
            raise ValueError("team_id values must be positive integers")
        parsed_values.append(value)
    return parsed_values


def _parse_team_list(raw_values: list[str]) -> list[Team] | None:
    team_ids = _parse_positive_int_list(raw_values)
    if team_ids is None:
        return None
    return [Team.from_id(team_id) for team_id in team_ids]


def _parse_season_list(
    raw_values: list[str],
    *,
    season_type: SeasonType,
) -> list[Season] | None:
    if not raw_values:
        return None
    return [Season(raw_value, season_type.value) for raw_value in raw_values]


def _serialize_rawr_metric_filters(filters: RawrMetricFilters) -> dict[str, Any]:
    return {
        "team": (
            None
            if filters.teams is None
            else [team.current.abbreviation for team in filters.teams]
        ),
        "team_id": None if filters.teams is None else [team.team_id for team in filters.teams],
        "season": None if filters.seasons is None else [season.id for season in filters.seasons],
        "season_type": filters.season_type.to_nba_format(),
        "min_average_minutes": filters.min_average_minutes,
        "min_total_minutes": filters.min_total_minutes,
        "top_n": filters.top_n,
        "min_games": filters.min_games,
        "ridge_alpha": filters.ridge_alpha,
    }


def _serialize_wowy_metric_filters(filters: WowyMetricFilters) -> dict[str, Any]:
    return {
        "team": (
            None
            if filters.teams is None
            else [team.current.abbreviation for team in filters.teams]
        ),
        "team_id": None if filters.teams is None else [team.team_id for team in filters.teams],
        "season": None if filters.seasons is None else [season.id for season in filters.seasons],
        "season_type": filters.season_type.to_nba_format(),
        "min_average_minutes": filters.min_average_minutes,
        "min_total_minutes": filters.min_total_minutes,
        "top_n": filters.top_n,
        "min_games_with": filters.min_games_with,
        "min_games_without": filters.min_games_without,
    }


__all__ = [
    "MetricExportResult",
    "MetricQueryRequest",
    "MetricViewResult",
    "build_metric_options_payload",
    "build_metric_query_export",
    "build_metric_query_view",
    "serialize_service_value",
]
