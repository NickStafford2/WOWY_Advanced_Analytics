from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from rawr_analytics.app.rawr._store import build_rawr_record_from_store_row
from rawr_analytics.app.rawr.presenters import (
    RawrQueryFiltersDTO,
    build_rawr_export_rows,
)
from rawr_analytics.app.rawr.presenters import (
    build_rawr_leaderboard_payload as build_rawr_leaderboard_payload_from_records,
)
from rawr_analytics.app.rawr.presenters import (
    build_rawr_player_seasons_payload as build_rawr_player_seasons_payload_from_records,
)
from rawr_analytics.app.rawr.presenters import (
    build_rawr_span_chart_payload as build_rawr_span_chart_payload_from_records,
)
from rawr_analytics.app.rawr.query import RawrQuery
from rawr_analytics.data.metric_store import load_rawr_player_season_value_rows
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
)
from rawr_analytics.metrics.rawr.records import (
    RawrPlayerSeasonRecord,
    build_player_season_records,
)
from rawr_analytics.services._metric_inputs import load_rawr_request
from rawr_analytics.services._metric_scope import (
    MetricStoreCatalog,
    build_metric_options_payload,
    build_metric_scope_key,
    require_current_metric_scope,
    season_ids,
)
from rawr_analytics.shared import JSONDict
from rawr_analytics.shared.season import Season

type RawrProgressFn = Callable[[int, int, Season], None]
type MetricQueryExport = list[JSONDict]
type RawrResultSource = Literal["cache", "live"]


@dataclass(frozen=True)
class ResolvedRawrResultDTO:
    rows: list[RawrPlayerSeasonRecord]
    seasons: list[str]
    source: RawrResultSource
    catalog: MetricStoreCatalog | None


def build_rawr_options_payload(query: RawrQuery) -> JSONDict:
    filters = RawrQueryFiltersDTO.from_query(query).for_options()
    return build_metric_options_payload(
        metric=Metric.RAWR,
        teams=query.teams,
        season_type=query.season_type,
        filters=filters.to_payload(),
    )


def resolve_rawr_result(
    query: RawrQuery,
    *,
    progress_fn: RawrProgressFn | None = None,
) -> ResolvedRawrResultDTO:
    if not query.recalculate:
        cached_result = _try_load_rawr_store_result(query)
        if cached_result is not None:
            return cached_result
    live_rows = _build_live_rawr_query_result(query, progress_fn=progress_fn)
    return ResolvedRawrResultDTO(
        rows=live_rows,
        seasons=_selected_rawr_seasons(query, live_rows),
        source="live",
        catalog=None,
    )


def build_rawr_leaderboard_payload(query: RawrQuery, result: ResolvedRawrResultDTO) -> JSONDict:
    payload = build_rawr_leaderboard_payload_from_records(
        metric=Metric.RAWR.value,
        rows=result.rows,
        seasons=result.seasons,
        top_n=query.top_n,
        mode=result.source,
        available_seasons=None if result.catalog is None else result.catalog.availability.seasons,
        available_teams=None if result.catalog is None else result.catalog.availability.teams,
    )
    payload["filters"] = RawrQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_rawr_player_seasons_payload(query: RawrQuery, result: ResolvedRawrResultDTO) -> JSONDict:
    payload = build_rawr_player_seasons_payload_from_records(result.rows)
    payload["filters"] = RawrQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_rawr_span_chart_payload(query: RawrQuery, result: ResolvedRawrResultDTO) -> JSONDict:
    payload = build_rawr_span_chart_payload_from_records(
        metric=Metric.RAWR.value,
        rows=result.rows,
        seasons=result.seasons,
        top_n=query.top_n,
    )
    payload["filters"] = RawrQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_rawr_leaderboard_export(result: ResolvedRawrResultDTO) -> MetricQueryExport:
    return build_rawr_export_rows(
        rows=result.rows,
        seasons=result.seasons,
    )


def _build_live_rawr_query_result(
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


def _try_load_rawr_store_result(query: RawrQuery) -> ResolvedRawrResultDTO | None:
    scope_key = build_metric_scope_key(query)
    try:
        catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
    except ValueError:
        return None
    rows = [
        build_rawr_record_from_store_row(row, season_type=query.season_type)
        for row in load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
    ]
    return ResolvedRawrResultDTO(
        rows=rows,
        seasons=_selected_rawr_seasons(query, rows),
        source="cache",
        catalog=catalog,
    )


def _selected_rawr_seasons(query: RawrQuery, rows: list[RawrPlayerSeasonRecord]) -> list[str]:
    if query.seasons is not None:
        return sorted({season.id for season in query.seasons})
    return sorted({row.season.id for row in rows})
