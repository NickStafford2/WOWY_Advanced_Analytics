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
from rawr_analytics.services._metric_scope import (
    build_metric_options_payload,
    build_metric_scope_key,
    build_metric_span_chart_payload,
    require_current_metric_scope,
    season_ids,
    selected_seasons,
)

MetricView = str
MetricQueryExport = tuple[str, list[dict[str, Any]]]


def build_rawr_options_payload(query: RawrQuery) -> dict[str, Any]:
    filters = build_options_filters_payload(_build_rawr_filters_payload(query))
    return build_metric_options_payload(
        metric=Metric.RAWR,
        teams=query.teams,
        season_type=query.season_type,
        filters=_serialize_rawr_filters(filters),
    )


def build_rawr_query_view(
    query: RawrQuery,
    *,
    view: MetricView,
) -> dict[str, Any]:
    payload = _build_rawr_view_payload(view=view, query=query)
    payload["filters"] = _serialize_rawr_filters(_build_rawr_filters_payload(query))
    return payload


def build_rawr_query_export(
    query: RawrQuery,
    *,
    view: MetricView,
) -> MetricQueryExport:
    scope_key = build_metric_scope_key(query)

    if view == "cached-leaderboard":
        catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        rows = load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
        return build_export_table(
            rows=cast(Sequence[Any], rows),
            seasons=selected_seasons(query.seasons, catalog),
        )

    if view == "custom-query":
        result = _build_rawr_custom_query_result(query)
        return build_export_table(
            rows=result.rows,
            seasons=sorted({row.season_id for row in result.rows}),
            metric_label=result.metric_label,
        )

    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_rawr_view_payload(
    *,
    view: MetricView,
    query: RawrQuery,
) -> dict[str, Any]:
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
        rows = load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
        payload = build_cached_leaderboard_payload(
            metric_label=catalog.metric_label,
            available_seasons=catalog.available_seasons,
            available_teams=catalog.available_teams,
            rows=cast(Sequence[Any], rows),
            seasons=selected_seasons(query.seasons, catalog),
            top_n=query.top_n,
        )
        payload["available_seasons"] = [season.id for season in catalog.available_seasons]
        payload["available_teams"] = [
            team.current.abbreviation for team in catalog.available_teams
        ]
        return payload

    if view == "span-chart":
        catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        return build_metric_span_chart_payload(
            metric=Metric.RAWR,
            catalog=catalog,
            scope_key=scope_key,
            top_n=query.top_n,
        )

    if view == "custom-query":
        result = _build_rawr_custom_query_result(query)
        return build_custom_leaderboard_payload(result, top_n=query.top_n)

    raise ValueError(f"Unknown metric view: {view}")


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


def _serialize_rawr_filters(filters: Any) -> dict[str, Any]:
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
