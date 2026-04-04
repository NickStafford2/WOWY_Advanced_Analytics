from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from rawr_analytics.app.wowy._store import build_wowy_value_from_store_row
from rawr_analytics.app.wowy.presenters import WowyQueryFiltersDTO
from rawr_analytics.app.wowy.presenters import (
    build_wowy_export_rows as build_wowy_export_rows_from_values,
)
from rawr_analytics.app.wowy.presenters import (
    build_wowy_leaderboard_payload as build_wowy_leaderboard_payload_from_values,
)
from rawr_analytics.app.wowy.presenters import (
    build_wowy_player_seasons_payload as build_wowy_player_seasons_payload_from_values,
)
from rawr_analytics.app.wowy.presenters import (
    build_wowy_span_chart_payload as build_wowy_span_chart_payload_from_values,
)
from rawr_analytics.app.wowy.query import WowyQuery
from rawr_analytics.data.metric_store import load_wowy_player_season_value_rows
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.inputs import build_wowy_season_inputs
from rawr_analytics.metrics.wowy.records import WowyPlayerSeasonValue, build_wowy_custom_query
from rawr_analytics.services._metric_inputs import load_wowy_records
from rawr_analytics.services._metric_scope import (
    MetricStoreCatalog,
    build_metric_options_payload,
    build_metric_scope_key,
    require_current_metric_scope,
    season_ids,
)
from rawr_analytics.shared import JSONDict

type WowyResultSource = Literal["cache", "live"]
type MetricQueryExport = list[JSONDict]


@dataclass(frozen=True)
class ResolvedWowyResultDTO:
    rows: list[WowyPlayerSeasonValue]
    seasons: list[str]
    source: WowyResultSource
    catalog: MetricStoreCatalog | None
    metric: Metric


def build_wowy_options_payload(
    query: WowyQuery,
    *,
    metric: Metric = Metric.WOWY,
) -> JSONDict:
    _require_wowy_metric(metric)
    filters = WowyQueryFiltersDTO.from_query(query).for_options()
    return build_metric_options_payload(
        metric=metric,
        teams=query.teams,
        season_type=query.season_type,
        filters=filters.to_payload(),
    )


def resolve_wowy_result(
    query: WowyQuery,
    *,
    metric: Metric = Metric.WOWY,
    recalculate: bool = False,
) -> ResolvedWowyResultDTO:
    _require_wowy_metric(metric)
    if not recalculate:
        cached_result = _try_load_wowy_store_result(metric=metric, query=query)
        if cached_result is not None:
            return cached_result
    live_rows = _build_live_wowy_query_result(metric=metric, query=query)
    return ResolvedWowyResultDTO(
        rows=live_rows,
        seasons=_selected_wowy_seasons(query, live_rows),
        source="live",
        catalog=None,
        metric=metric,
    )


def build_wowy_leaderboard_payload(
    query: WowyQuery,
    result: ResolvedWowyResultDTO,
) -> JSONDict:
    payload = build_wowy_leaderboard_payload_from_values(
        metric=result.metric,
        rows=result.rows,
        seasons=result.seasons,
        top_n=query.top_n,
        mode=result.source,
        available_seasons=None if result.catalog is None else result.catalog.availability.seasons,
        available_teams=None if result.catalog is None else result.catalog.availability.teams,
    )
    payload["filters"] = WowyQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_wowy_player_seasons_payload(
    query: WowyQuery,
    result: ResolvedWowyResultDTO,
) -> JSONDict:
    payload = build_wowy_player_seasons_payload_from_values(
        metric=result.metric,
        rows=result.rows,
    )
    payload["filters"] = WowyQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_wowy_span_chart_payload(
    query: WowyQuery,
    result: ResolvedWowyResultDTO,
) -> JSONDict:
    payload = build_wowy_span_chart_payload_from_values(
        metric=result.metric,
        rows=result.rows,
        seasons=result.seasons,
        top_n=query.top_n,
    )
    payload["filters"] = WowyQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_wowy_leaderboard_export(
    query: WowyQuery,
    result: ResolvedWowyResultDTO,
) -> MetricQueryExport:
    return build_wowy_export_rows_from_values(
        rows=result.rows,
        seasons=result.seasons,
    )


def _build_live_wowy_query_result(
    *,
    metric: Metric,
    query: WowyQuery,
) -> list[WowyPlayerSeasonValue]:
    games, game_players = load_wowy_records(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
    )
    season_inputs = build_wowy_season_inputs(games=games, game_players=game_players)
    return build_wowy_custom_query(
        metric,
        season_inputs=season_inputs,
        min_games_with=query.min_games_with,
        min_games_without=query.min_games_without,
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
    )


def _try_load_wowy_store_result(
    *,
    metric: Metric,
    query: WowyQuery,
) -> ResolvedWowyResultDTO | None:
    scope_key = build_metric_scope_key(query)
    try:
        catalog = require_current_metric_scope(metric=metric, scope_key=scope_key)
    except ValueError:
        return None
    rows = [
        build_wowy_value_from_store_row(row)
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
    return ResolvedWowyResultDTO(
        rows=rows,
        seasons=_selected_wowy_seasons(query, rows),
        source="cache",
        catalog=catalog,
        metric=metric,
    )


def _selected_wowy_seasons(
    query: WowyQuery,
    rows: list[WowyPlayerSeasonValue],
) -> list[str]:
    if query.seasons is not None:
        return sorted({season.id for season in query.seasons})
    return sorted({row.season_id for row in rows})


def _require_wowy_metric(metric: Metric) -> None:
    if metric not in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        raise ValueError(f"Unknown WOWY metric: {metric}")
