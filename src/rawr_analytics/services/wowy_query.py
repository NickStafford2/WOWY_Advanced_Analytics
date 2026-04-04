from __future__ import annotations

from typing import cast

from rawr_analytics.data.metric_store import (
    load_wowy_player_season_value_rows,
)
from rawr_analytics.metrics import MetricView
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy import (
    WowyCustomQueryResult,
    WowyPlayerSeasonValue,
    WowyPlayerValue,
    WowyQuery,
    WowyQueryFilters,
    build_export_table,
    build_leaderboard_payload,
    build_player_seasons_payload,
    build_wowy_custom_query,
)
from rawr_analytics.services._metric_inputs import load_wowy_season_inputs
from rawr_analytics.services._metric_scope import (
    build_metric_options_payload,
    build_metric_scope_key,
    build_metric_span_chart_payload,
    require_current_metric_scope,
    season_ids,
    selected_seasons,
)
from rawr_analytics.shared import JSONDict
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary

type MetricQueryExport = tuple[str, list[JSONDict]]


def build_wowy_options_payload(
    metric: Metric,
    query: WowyQuery,
) -> JSONDict:
    _require_wowy_metric(metric)
    filters = WowyQueryFilters.from_query(query).for_options()
    return cast(
        JSONDict,
        build_metric_options_payload(
            metric=metric,
            teams=query.teams,
            season_type=query.season_type,
            filters=filters.to_payload(),
        ),
    )


def build_wowy_query_view(
    metric: Metric,
    query: WowyQuery,
    *,
    view: MetricView,
) -> JSONDict:
    _require_wowy_metric(metric)
    filters = WowyQueryFilters.from_query(query).to_payload()
    payload = _build_wowy_view_payload(metric, view=view, query=query)
    payload["filters"] = filters
    return payload


def build_wowy_query_export(
    metric: Metric,
    query: WowyQuery,
    view: MetricView,
) -> MetricQueryExport:
    _require_wowy_metric(metric)
    scope_key = build_metric_scope_key(query)

    match view:
        case "cached-leaderboard":
            catalog = require_current_metric_scope(metric=metric, scope_key=scope_key)
            return cast(
                MetricQueryExport,
                build_export_table(
                    metric,
                    rows=_load_wowy_store_values(metric, query, scope_key=scope_key),
                    seasons=selected_seasons(query.seasons, catalog),
                ),
            )
        case "custom-query":
            result = _build_wowy_custom_query_result(metric, query)
            return cast(
                MetricQueryExport,
                build_export_table(
                    metric,
                    rows=result.rows,
                    seasons=sorted({row.season_id for row in result.rows}),
                    metric_label=result.metric_label,
                ),
            )
        case _:
            raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_wowy_view_payload(
    metric: Metric,
    *,
    view: MetricView,
    query: WowyQuery,
) -> JSONDict:
    scope_key = build_metric_scope_key(query)

    match view:
        case "player-seasons":
            require_current_metric_scope(metric=metric, scope_key=scope_key)
            return cast(
                JSONDict,
                build_player_seasons_payload(
                    metric,
                    _load_wowy_store_values(metric, query, scope_key=scope_key),
                ),
            )
        case "cached-leaderboard":
            catalog = require_current_metric_scope(metric=metric, scope_key=scope_key)
            return cast(
                JSONDict,
                build_leaderboard_payload(
                    metric=metric,
                    metric_label=catalog.metric_label,
                    rows=_load_wowy_store_values(metric, query, scope_key=scope_key),
                    seasons=selected_seasons(query.seasons, catalog),
                    top_n=query.top_n,
                    mode="cached",
                    available_seasons=catalog.availability.seasons,
                    available_teams=catalog.availability.teams,
                ),
            )
        case "span-chart":
            catalog = require_current_metric_scope(metric=metric, scope_key=scope_key)
            return cast(
                JSONDict,
                build_metric_span_chart_payload(
                    metric=metric,
                    catalog=catalog,
                    scope_key=scope_key,
                    top_n=query.top_n,
                ),
            )
        case "custom-query":
            result = _build_wowy_custom_query_result(metric, query)
            return cast(
                JSONDict,
                build_leaderboard_payload(
                    metric=metric.value,
                    metric_label=result.metric_label,
                    rows=result.rows,
                    seasons=sorted({row.season_id for row in result.rows}),
                    top_n=query.top_n,
                    mode="custom",
                ),
            )
        case _:
            raise ValueError(f"Unknown metric view: {view}")


def _build_wowy_custom_query_result(
    metric: Metric,
    query: WowyQuery,
) -> WowyCustomQueryResult:
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


def _load_wowy_store_values(
    metric: Metric,
    query: WowyQuery,
    *,
    scope_key: str,
) -> list[WowyPlayerSeasonValue]:
    return [
        WowyPlayerSeasonValue(
            season_id=row.season_id,
            player=PlayerSummary(
                player_id=row.player_id,
                player_name=row.player_name,
            ),
            minutes=PlayerMinutes(
                average_minutes=row.average_minutes,
                total_minutes=row.total_minutes,
            ),
            result=WowyPlayerValue(
                games_with=row.games_with,
                games_without=row.games_without,
                avg_margin_with=row.avg_margin_with,
                avg_margin_without=row.avg_margin_without,
                value=row.value,
                raw_value=row.raw_wowy_score,
            ),
        )
        for row in load_wowy_player_season_value_rows(
            metric_id=metric.value,
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )
    ]


def _require_wowy_metric(metric: Metric) -> None:
    if metric not in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        raise ValueError(f"Unknown WOWY metric: {metric}")
