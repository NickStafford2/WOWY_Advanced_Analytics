from __future__ import annotations

from collections.abc import Callable

from rawr_analytics.app.rawr._store import build_rawr_record_from_store_row
from rawr_analytics.app.rawr.presenters import (
    RawrQueryFiltersDTO,
    build_rawr_export_rows,
    build_rawr_leaderboard_payload,
    build_rawr_player_seasons_payload,
)
from rawr_analytics.app.rawr.query import RawrQuery
from rawr_analytics.data.metric_store import load_rawr_player_season_value_rows
from rawr_analytics.metrics import MetricView
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    RawrPlayerSeasonRecord,
    build_player_season_records,
)
from rawr_analytics.services._metric_inputs import load_rawr_request
from rawr_analytics.services._metric_scope import (
    build_metric_options_payload,
    build_metric_scope_key,
    build_metric_span_chart_payload,
    require_current_metric_scope,
    season_ids,
    selected_seasons,
)
from rawr_analytics.shared import JSONDict
from rawr_analytics.shared.season import Season

type RawrProgressFn = Callable[[int, int, Season], None]
type MetricQueryExport = list[JSONDict]


def build_rawr_options_payload(query: RawrQuery) -> JSONDict:
    filters = RawrQueryFiltersDTO.from_query(query).for_options()
    return build_metric_options_payload(
        metric=Metric.RAWR,
        teams=query.teams,
        season_type=query.season_type,
        filters=filters.to_payload(),
    )


def build_rawr_query_view(
    query: RawrQuery,
    *,
    view: MetricView,
) -> JSONDict:
    payload = _build_rawr_view_payload(view=view, query=query)
    payload["filters"] = RawrQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_rawr_query_export(
    query: RawrQuery,
    *,
    view: MetricView,
    progress_fn: RawrProgressFn | None = None,
) -> MetricQueryExport:
    scope_key = build_metric_scope_key(query)

    match view:
        case "cached-leaderboard":
            catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
            return build_rawr_export_rows(
                rows=_load_rawr_store_values(query, scope_key=scope_key),
                seasons=selected_seasons(query.seasons, catalog),
            )
        case "custom-query":
            rows = _build_rawr_custom_query_result(query, progress_fn=progress_fn)
            return build_rawr_export_rows(
                rows=rows,
                seasons=sorted({row.season.id for row in rows}),
            )
        case _:
            raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_rawr_view_payload(
    *,
    view: MetricView,
    query: RawrQuery,
) -> JSONDict:
    scope_key = build_metric_scope_key(query)

    match view:
        case "player-seasons":
            require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
            return build_rawr_player_seasons_payload(
                _load_rawr_store_values(query, scope_key=scope_key)
            )
        case "cached-leaderboard":
            catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
            return build_rawr_leaderboard_payload(
                metric=Metric.RAWR.value,
                rows=_load_rawr_store_values(query, scope_key=scope_key),
                seasons=selected_seasons(query.seasons, catalog),
                top_n=query.top_n,
                mode="cached",
                available_seasons=catalog.availability.seasons,
                available_teams=catalog.availability.teams,
            )
        case "span-chart":
            catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
            return build_metric_span_chart_payload(
                metric=Metric.RAWR,
                catalog=catalog,
                scope_key=scope_key,
                top_n=query.top_n,
            )
        case "custom-query":
            rows = _build_rawr_custom_query_result(query)
            return build_rawr_leaderboard_payload(
                metric=Metric.RAWR.value,
                rows=rows,
                seasons=sorted({row.season.id for row in rows}),
                top_n=query.top_n,
                mode="custom",
            )
        case _:
            raise ValueError(f"Unknown metric view: {view}")


def _build_rawr_custom_query_result(
    query: RawrQuery,
    *,
    progress_fn: RawrProgressFn | None = None,
) -> list[RawrPlayerSeasonRecord]:
    request = load_rawr_request(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
        min_games=query.min_games,
        ridge_alpha=query.ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        progress_fn=progress_fn,
    )
    return build_player_season_records(request)


def _load_rawr_store_values(
    query: RawrQuery,
    *,
    scope_key: str,
) -> list[RawrPlayerSeasonRecord]:
    return [
        build_rawr_record_from_store_row(row, season_type=query.season_type)
        for row in load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
    ]
