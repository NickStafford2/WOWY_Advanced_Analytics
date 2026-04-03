from __future__ import annotations

from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import RawrQueryFilters
from rawr_analytics.metrics.wowy import WowyQueryFilters
from rawr_analytics.services._metric_query_shared import (
    MetricExportResult,
    MetricOptionsPayload,
    MetricViewResult,
    TeamOption,
)
from rawr_analytics.services.rawr_query import (
    build_rawr_options_payload,
    build_rawr_query_export,
    build_rawr_query_view,
)
from rawr_analytics.services.wowy_query import (
    build_wowy_options_payload,
    build_wowy_query_export,
    build_wowy_query_view,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def serialize_service_value(value: Any) -> Any:
    if isinstance(value, Team):
        return value.current.abbreviation
    if isinstance(value, Season):
        return value.id
    if isinstance(value, SeasonType):
        return value.to_nba_format()
    if isinstance(value, Metric):
        return value.value
    if isinstance(value, RawrQueryFilters):
        return _serialize_rawr_metric_filters(value)
    if isinstance(value, WowyQueryFilters):
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


def _serialize_rawr_metric_filters(filters: RawrQueryFilters) -> dict[str, Any]:
    return {
        "team": (
            None if filters.teams is None else [team.current.abbreviation for team in filters.teams]
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


def _serialize_wowy_metric_filters(filters: WowyQueryFilters) -> dict[str, Any]:
    return {
        "team": (
            None if filters.teams is None else [team.current.abbreviation for team in filters.teams]
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
    "MetricViewResult",
    "build_rawr_options_payload",
    "build_rawr_query_export",
    "build_rawr_query_view",
    "build_wowy_options_payload",
    "build_wowy_query_export",
    "build_wowy_query_view",
    "serialize_service_value",
]
