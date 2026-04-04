from __future__ import annotations

from collections.abc import Callable
from typing import Literal

from rawr_analytics.app.rawr._store import build_rawr_record_from_store_row
from rawr_analytics.app.rawr.presenters import (
    RawrQueryFiltersDTO,
    build_rawr_export_rows,
    build_rawr_leaderboard_payload,
    build_rawr_player_seasons_payload,
    build_rawr_span_chart_payload,
)
from rawr_analytics.app.rawr.query import RawrQuery
from rawr_analytics.data.metric_store import load_rawr_player_season_value_rows
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
)
from rawr_analytics.shared import JSONDict
from rawr_analytics.shared.season import Season

type RawrProgressFn = Callable[[int, int, Season], None]
type MetricQueryExport = list[JSONDict]
type RawrView = Literal["leaderboard", "player-seasons", "span-chart"]


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
    view: RawrView,
) -> JSONDict:
    payload = _build_rawr_view_payload(view=view, query=query)
    payload["filters"] = RawrQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_rawr_query_export(
    query: RawrQuery,
    *,
    view: RawrView,
    progress_fn: RawrProgressFn | None = None,
) -> MetricQueryExport:
    match view:
        case "leaderboard":
            rows = _resolve_rawr_rows(query, progress_fn=progress_fn)
            seasons = _selected_rawr_seasons(query, rows)
            return build_rawr_export_rows(
                rows=rows,
                seasons=seasons,
            )
        case _:
            raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_rawr_view_payload(
    *,
    view: RawrView,
    query: RawrQuery,
) -> JSONDict:
    scope_key = build_metric_scope_key(query)

    match view:
        case "player-seasons":
            return build_rawr_player_seasons_payload(_resolve_rawr_rows(query))
        case "leaderboard":
            rows = _resolve_rawr_rows(query)
            seasons = _selected_rawr_seasons(query, rows)
            catalog = _try_require_current_metric_scope(query)
            return build_rawr_leaderboard_payload(
                metric=Metric.RAWR.value,
                rows=rows,
                seasons=seasons,
                top_n=query.top_n,
                mode="recalculated" if query.recalculate else "resolved",
                available_seasons=None if catalog is None else catalog.availability.seasons,
                available_teams=None if catalog is None else catalog.availability.teams,
            )
        case "span-chart":
            if not query.recalculate:
                try:
                    catalog = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
                    return build_metric_span_chart_payload(
                        metric=Metric.RAWR,
                        catalog=catalog,
                        scope_key=scope_key,
                        top_n=query.top_n,
                    )
                except ValueError:
                    pass
            rows = _build_live_rawr_query_result(query)
            seasons = _selected_rawr_seasons(query, rows)
            return build_rawr_span_chart_payload(
                metric=Metric.RAWR.value,
                rows=rows,
                seasons=seasons,
                top_n=query.top_n,
            )
        case _:
            raise ValueError(f"Unknown metric view: {view}")


def _resolve_rawr_rows(
    query: RawrQuery,
    *,
    progress_fn: RawrProgressFn | None = None,
) -> list[RawrPlayerSeasonRecord]:
    if not query.recalculate:
        cached_rows = _try_load_rawr_store_values(query)
        if cached_rows is not None:
            return cached_rows
    return _build_live_rawr_query_result(query, progress_fn=progress_fn)


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


def _try_load_rawr_store_values(
    query: RawrQuery,
 ) -> list[RawrPlayerSeasonRecord] | None:
    scope_key = build_metric_scope_key(query)
    try:
        require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
    except ValueError:
        return None
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


def _selected_rawr_seasons(
    query: RawrQuery,
    rows: list[RawrPlayerSeasonRecord],
) -> list[str]:
    if query.seasons is not None:
        return sorted({season.id for season in query.seasons})
    return sorted({row.season.id for row in rows})


def _try_require_current_metric_scope(query: RawrQuery):
    scope_key = build_metric_scope_key(query)
    try:
        return require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
    except ValueError:
        return None
