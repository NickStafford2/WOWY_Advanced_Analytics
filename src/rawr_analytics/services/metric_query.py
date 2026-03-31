from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.metric_query import (
    MetricOptionsPayload,
    MetricQuery,
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


__all__ = [
    "MetricExportResult",
    "MetricQueryRequest",
    "MetricViewResult",
    "build_metric_options_payload",
    "build_metric_query_export",
    "build_metric_query_view",
]
