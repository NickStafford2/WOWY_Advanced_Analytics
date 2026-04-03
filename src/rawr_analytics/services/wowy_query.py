from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from rawr_analytics.data.metric_store import load_wowy_player_season_value_rows
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy import (
    WowyQuery,
    build_cached_leaderboard_payload,
    build_custom_leaderboard_payload,
    build_export_table,
    build_options_filters_payload,
    build_player_seasons_payload,
    build_query_filters_payload,
    build_wowy_custom_query,
)
from rawr_analytics.services._metric_inputs import load_wowy_season_inputs
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
    require_wowy_metric,
    season_ids,
)


def build_wowy_options_payload(
    metric: Metric,
    query: WowyQuery,
) -> MetricOptionsPayload:
    require_wowy_metric(metric)
    snapshot = build_metric_store_scope_snapshot(
        metric,
        teams=query.teams,
        season_type=query.season_type,
    )
    filters = build_options_filters_payload(_build_wowy_filters_payload(query))
    return build_options_payload(snapshot, filters=filters)


def build_wowy_query_view(
    metric: Metric,
    query: WowyQuery,
    *,
    view: MetricView,
) -> MetricViewResult:
    require_wowy_metric(metric)
    payload = _build_wowy_view_payload(metric, view=view, query=query)
    payload["filters"] = _build_wowy_filters_payload(query)
    return MetricViewResult(
        metric=metric,
        view=view,
        query=query,
        payload=payload,
    )


def build_wowy_query_export(
    metric: Metric,
    query: WowyQuery,
    view: MetricView,
) -> MetricExportResult:
    require_wowy_metric(metric)
    metric_label, rows = _build_wowy_export_table_payload(metric, view=view, query=query)
    return MetricExportResult(
        metric=metric,
        view=view,
        query=query,
        metric_label=metric_label,
        rows=rows,
    )


def _build_wowy_filters_payload(query: WowyQuery):
    return build_query_filters_payload(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        top_n=query.top_n,
        min_games_with=query.min_games_with,
        min_games_without=query.min_games_without,
    )


def _build_wowy_view_payload(
    metric: Metric,
    *,
    view: MetricView,
    query: WowyQuery,
) -> dict[str, object]:
    require_wowy_metric(metric)
    scope_key = build_metric_scope_key(query)

    if view == "player-seasons":
        require_current_metric_scope(metric=metric, scope_key=scope_key)
        rows = load_wowy_player_season_value_rows(
            metric_id=metric.value,
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )
        return build_player_seasons_payload(metric, cast(Sequence[Any], rows))

    if view == "cached-leaderboard":
        catalog = require_current_metric_scope(metric=metric, scope_key=scope_key)
        selected_seasons = season_ids(query.seasons) or [
            season.id for season in catalog.available_seasons
        ]
        rows = load_wowy_player_season_value_rows(
            metric_id=metric.value,
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )
        return build_cached_leaderboard_payload(
            metric,
            metric_label=catalog.metric_label,
            available_seasons=catalog.available_seasons,
            available_teams=catalog.available_teams,
            rows=cast(Sequence[Any], rows),
            seasons=selected_seasons,
            top_n=query.top_n,
        )

    if view == "span-chart":
        catalog = require_current_metric_scope(metric=metric, scope_key=scope_key)
        return build_span_chart_payload_for_scope(
            metric=metric,
            catalog=catalog,
            scope_key=scope_key,
            top_n=query.top_n,
        )

    if view == "custom-query":
        result = _build_wowy_custom_query_result(metric, query)
        return build_custom_leaderboard_payload(metric, result, top_n=query.top_n)

    raise ValueError(f"Unknown metric view: {view}")


def _build_wowy_export_table_payload(
    metric: Metric,
    *,
    view: MetricView,
    query: WowyQuery,
) -> tuple[str, list[dict[str, object]]]:
    require_wowy_metric(metric)
    scope_key = build_metric_scope_key(query)

    if view == "cached-leaderboard":
        catalog = require_current_metric_scope(metric=metric, scope_key=scope_key)
        seasons = season_ids(query.seasons) or [season.id for season in catalog.available_seasons]
        rows = load_wowy_player_season_value_rows(
            metric_id=metric.value,
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )
        return build_export_table(metric, rows=cast(Sequence[Any], rows), seasons=seasons)

    if view == "custom-query":
        result = _build_wowy_custom_query_result(metric, query)
        seasons = sorted({row.season_id for row in result.rows})
        return build_export_table(
            metric,
            rows=result.rows,
            seasons=seasons,
            metric_label=result.metric_label,
        )

    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_wowy_custom_query_result(metric: Metric, query: WowyQuery):
    require_wowy_metric(metric)
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


__all__ = [
    "build_wowy_options_payload",
    "build_wowy_query_export",
    "build_wowy_query_view",
]
