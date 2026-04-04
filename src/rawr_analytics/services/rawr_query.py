from __future__ import annotations

from collections.abc import Callable
from typing import cast

from rawr_analytics.data.metric_store import (
    RawrPlayerSeasonValueRow,
    load_rawr_player_season_value_rows,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import (
    RawrCustomQueryResult,
    RawrPlayerSeasonValue,
    RawrQuery,
    RawrQueryFilters,
    RawrValue,
    build_cached_leaderboard_payload,
    build_custom_leaderboard_payload,
    build_export_table,
    build_player_seasons_payload,
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
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season

type JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]
type RawrProgressFn = Callable[[int, int, Season], None]

MetricView = str
MetricQueryExport = tuple[str, list[dict[str, JSONValue]]]


def build_rawr_options_payload(query: RawrQuery) -> dict[str, JSONValue]:
    filters = RawrQueryFilters.from_query(query).for_options()
    return cast(
        dict[str, JSONValue],
        build_metric_options_payload(
            metric=Metric.RAWR,
            teams=query.teams,
            season_type=query.season_type,
            filters=filters.to_payload(),
        ),
    )


def build_rawr_query_view(
    query: RawrQuery,
    *,
    view: MetricView,
) -> dict[str, JSONValue]:
    payload = _build_rawr_view_payload(view=view, query=query)
    payload["filters"] = RawrQueryFilters.from_query(query).to_payload()
    return payload


def build_rawr_query_export(
    query: RawrQuery,
    *,
    view: MetricView,
    progress_fn: RawrProgressFn | None = None,
) -> MetricQueryExport:
    scope_key = build_metric_scope_key(query)

    if view == "cached-leaderboard":
        catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        values = _load_rawr_store_values(query, scope_key=scope_key)
        return cast(
            MetricQueryExport,
            build_export_table(
                rows=values,
                seasons=selected_seasons(query.seasons, catalog),
            ),
        )

    if view == "custom-query":
        result = _build_rawr_custom_query_result(query, progress_fn=progress_fn)
        return cast(
            MetricQueryExport,
            build_export_table(
                rows=result.rows,
                seasons=sorted({row.season_id for row in result.rows}),
                metric_label=result.metric_label,
            ),
        )

    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_rawr_view_payload(
    *,
    view: MetricView,
    query: RawrQuery,
) -> dict[str, JSONValue]:
    scope_key = build_metric_scope_key(query)

    if view == "player-seasons":
        require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        values = _load_rawr_store_values(query, scope_key=scope_key)
        return cast(
            dict[str, JSONValue],
            build_player_seasons_payload(values),
        )

    if view == "cached-leaderboard":
        catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        return cast(
            dict[str, JSONValue],
            build_cached_leaderboard_payload(
                metric_label=catalog.metric_label,
                available_seasons=catalog.availability.seasons,
                available_teams=catalog.availability.teams,
                rows=_load_rawr_store_values(query, scope_key=scope_key),
                seasons=selected_seasons(query.seasons, catalog),
                top_n=query.top_n,
            ),
        )

    if view == "span-chart":
        catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        return cast(
            dict[str, JSONValue],
            build_metric_span_chart_payload(
                metric=Metric.RAWR,
                catalog=catalog,
                scope_key=scope_key,
                top_n=query.top_n,
            ),
        )

    if view == "custom-query":
        result = _build_rawr_custom_query_result(query)
        return cast(
            dict[str, JSONValue],
            build_custom_leaderboard_payload(result, top_n=query.top_n),
        )

    raise ValueError(f"Unknown metric view: {view}")


def _build_rawr_custom_query_result(
    query: RawrQuery,
    *,
    progress_fn: RawrProgressFn | None = None,
) -> RawrCustomQueryResult:
    season_inputs = load_rawr_season_inputs(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
        progress_fn=progress_fn,
    )
    return build_rawr_custom_query(
        season_inputs=season_inputs,
        min_games=query.min_games,
        ridge_alpha=query.ridge_alpha,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
    )


def _build_rawr_store_values(
    rows: list[RawrPlayerSeasonValueRow],
) -> list[RawrPlayerSeasonValue]:
    return [
        RawrPlayerSeasonValue(
            season_id=row.season_id,
            player=PlayerSummary(
                player_id=row.player_id,
                player_name=row.player_name,
            ),
            minutes=PlayerMinutes(
                average_minutes=row.average_minutes,
                total_minutes=row.total_minutes,
            ),
            result=RawrValue(
                games=row.games,
                coefficient=row.coefficient,
            ),
        )
        for row in rows
    ]

def _load_rawr_store_values(
    query: RawrQuery,
    *,
    scope_key: str,
) -> list[RawrPlayerSeasonValue]:
    return _build_rawr_store_values(
        load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
    )
