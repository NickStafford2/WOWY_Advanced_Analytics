from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from rawr_analytics.data.metric_store import load_rawr_player_season_value_rows
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import (
    RawrQuery,
    build_cached_leaderboard_payload,
    build_custom_leaderboard_payload,
    build_export_table,
    build_options_filters_payload,
    build_player_seasons_payload,
    build_query_filters_payload,
    build_rawr_custom_query,
)
from rawr_analytics.services._metric_inputs import load_rawr_season_inputs
from rawr_analytics.services._metric_query_shared import (
    MetricExportResult,
    MetricOptionsPayload,
    MetricView,
    MetricViewResult,
    build_metric_scope_key,
    build_metric_store_scope_snapshot,
    build_options_payload,
    build_span_chart_payload_for_scope,
    require_current_metric_scope,
    season_ids,
)


def build_rawr_options_payload(query: RawrQuery) -> MetricOptionsPayload:
    snapshot = build_metric_store_scope_snapshot(
        Metric.RAWR,
        teams=query.teams,
        season_type=query.season_type,
    )
    filters = build_options_filters_payload(_build_rawr_filters_payload(query))
    return build_options_payload(snapshot, filters=filters)


def build_rawr_query_view(
    query: RawrQuery,
    *,
    view: MetricView,
) -> MetricViewResult:
    payload = _build_rawr_view_payload(view=view, query=query)
    payload["filters"] = _build_rawr_filters_payload(query)
    return MetricViewResult(
        metric=Metric.RAWR,
        view=view,
        query=query,
        payload=payload,
    )


def build_rawr_query_export(
    query: RawrQuery,
    *,
    view: MetricView,
) -> MetricExportResult:
    metric_label, rows = _build_rawr_export_table_payload(view=view, query=query)
    return MetricExportResult(
        metric=Metric.RAWR,
        view=view,
        query=query,
        metric_label=metric_label,
        rows=rows,
    )


def _build_rawr_filters_payload(query: RawrQuery):
    return build_query_filters_payload(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        top_n=query.top_n,
        min_games=query.min_games,
        ridge_alpha=query.ridge_alpha,
    )


def _build_rawr_view_payload(
    *,
    view: MetricView,
    query: RawrQuery,
) -> dict[str, object]:
    scope_key = build_metric_scope_key(query)

    if view == "player-seasons":
        require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        rows = load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
        return build_player_seasons_payload(cast(Sequence[Any], rows))

    if view == "cached-leaderboard":
        catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        selected_seasons = season_ids(query.seasons) or [
            season.id for season in catalog.available_seasons
        ]
        rows = load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
        return build_cached_leaderboard_payload(
            metric_label=catalog.metric_label,
            available_seasons=catalog.available_seasons,
            available_teams=catalog.available_teams,
            rows=cast(Sequence[Any], rows),
            seasons=selected_seasons,
            top_n=query.top_n,
        )

    if view == "span-chart":
        catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        return build_span_chart_payload_for_scope(
            metric=Metric.RAWR,
            catalog=catalog,
            scope_key=scope_key,
            top_n=query.top_n,
        )

    if view == "custom-query":
        result = _build_rawr_custom_query_result(query)
        return build_custom_leaderboard_payload(result, top_n=query.top_n)

    raise ValueError(f"Unknown metric view: {view}")


def _build_rawr_export_table_payload(
    *,
    view: MetricView,
    query: RawrQuery,
) -> tuple[str, list[dict[str, object]]]:
    scope_key = build_metric_scope_key(query)

    if view == "cached-leaderboard":
        catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        seasons = season_ids(query.seasons) or [season.id for season in catalog.available_seasons]
        rows = load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
        return build_export_table(rows=cast(Sequence[Any], rows), seasons=seasons)

    if view == "custom-query":
        result = _build_rawr_custom_query_result(query)
        seasons = sorted({row.season_id for row in result.rows})
        return build_export_table(
            rows=result.rows,
            seasons=seasons,
            metric_label=result.metric_label,
        )

    raise ValueError(f"Metric view {view!r} does not support CSV export")


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


__all__ = [
    "build_rawr_options_payload",
    "build_rawr_query_export",
    "build_rawr_query_view",
]
