from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from typing import Any

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.metric_query import (
    MetricOptionsPayload,
    MetricQuery,
    RawrMetricFilters,
    TeamOption,
    WowyMetricFilters,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_export_table as _build_metric_export_table,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_options_payload as _build_metric_options_payload,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_query as _build_metric_query,
)
from rawr_analytics.metrics.metric_query import (
    build_metric_view_payload as _build_metric_view_payload,
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


def parse_metric_query_request(
    *,
    metric: str,
    season_type: str,
    team_ids: list[str] | None = None,
    seasons: list[str] | None = None,
    top_n: str | None = None,
    min_average_minutes: str | None = None,
    min_total_minutes: str | None = None,
    min_games: str | None = None,
    ridge_alpha: str | None = None,
    min_games_with: str | None = None,
    min_games_without: str | None = None,
) -> MetricQueryRequest:
    parsed_season_type = SeasonType.parse(season_type)
    return MetricQueryRequest(
        metric=Metric.parse(metric),
        season_type=parsed_season_type,
        teams=_parse_team_list(team_ids),
        seasons=_parse_season_list(seasons, season_type=parsed_season_type),
        top_n=_parse_optional_int(top_n),
        min_average_minutes=_parse_optional_float(min_average_minutes),
        min_total_minutes=_parse_optional_float(min_total_minutes),
        min_games=_parse_optional_int(min_games),
        ridge_alpha=_parse_optional_float(ridge_alpha),
        min_games_with=_parse_optional_int(min_games_with),
        min_games_without=_parse_optional_int(min_games_without),
    )


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
            field.name: serialize_service_value(getattr(value, field.name)) for field in fields(value)
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
    return _build_metric_options_payload(
        request.metric,
        teams=query.teams,
        season_type=query.season_type,
    )


def build_metric_query_view(
    request: MetricQueryRequest,
    *,
    view: MetricView,
) -> MetricViewResult:
    query = _build_query(request)
    payload = _build_metric_view_payload(
        request.metric,
        view=view,
        query=query,
    )
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
    metric_label, rows = _build_metric_export_table(
        request.metric,
        view=view,
        query=query,
    )
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


def _parse_optional_int(raw_value: str | None) -> int | None:
    return None if raw_value is None else int(raw_value)


def _parse_optional_float(raw_value: str | None) -> float | None:
    return None if raw_value is None else float(raw_value)


def _parse_positive_int_list(raw_values: list[str] | None) -> list[int] | None:
    if not raw_values:
        return None
    parsed_values: list[int] = []
    for raw_value in raw_values:
        value = int(raw_value)
        if value <= 0:
            raise ValueError("team_id values must be positive integers")
        parsed_values.append(value)
    return parsed_values


def _parse_team_list(raw_values: list[str] | None) -> list[Team] | None:
    team_ids = _parse_positive_int_list(raw_values)
    if team_ids is None:
        return None
    return [Team.from_id(team_id) for team_id in team_ids]


def _parse_season_list(
    raw_values: list[str] | None,
    *,
    season_type: SeasonType,
) -> list[Season] | None:
    if not raw_values:
        return None
    return [Season(raw_value, season_type.value) for raw_value in raw_values]


def _serialize_rawr_metric_filters(filters: RawrMetricFilters) -> dict[str, Any]:
    return {
        "team": None if filters.teams is None else [team.current.abbreviation for team in filters.teams],
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
        "team": None if filters.teams is None else [team.current.abbreviation for team in filters.teams],
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
    "parse_metric_query_request",
    "serialize_service_value",
]
