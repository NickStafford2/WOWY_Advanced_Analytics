from __future__ import annotations

from typing import Any

from rawr_analytics.data.metric_store import (
    MetricScopeCatalogRow,
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
    load_metric_span_store_rows,
    load_rawr_player_season_value_rows,
    load_wowy_player_season_value_rows,
)
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter, season_ids
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import build_rawr_custom_query
from rawr_analytics.metrics.rawr import default_filters as _rawr_default_filters
from rawr_analytics.metrics.rawr import describe_metric as describe_rawr_metric
from rawr_analytics.metrics.rawr.models import RawrCustomQueryResult, RawrCustomQueryRow
from rawr_analytics.metrics.wowy import build_wowy_custom_query
from rawr_analytics.metrics.wowy import describe_metric as describe_wowy_metric
from rawr_analytics.metrics.wowy.models import WowyCustomQueryResult, WowyCustomQueryRow
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

from .models import MetricQuery
from .scope import build_filters_payload, require_current_metric_scope

MetricView = str


def build_metric_view_payload(
    metric: Metric,
    *,
    view: MetricView,
    query: MetricQuery,
) -> dict[str, Any]:
    if metric == Metric.RAWR:
        payload = _build_rawr_metric_view_payload(view=view, query=query)
    elif metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        payload = _build_wowy_metric_view_payload(metric=metric, view=view, query=query)
    else:
        raise ValueError(f"Unknown metric: {metric}")
    payload["filters"] = build_filters_payload(query)
    return payload


def build_metric_export_table(
    metric: Metric,
    *,
    view: MetricView,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    if metric == Metric.RAWR:
        return _build_rawr_metric_export_table(view=view, query=query)
    if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        return _build_wowy_metric_export_table(metric=metric, view=view, query=query)
    raise ValueError(f"Unknown metric: {metric}")


def _build_rawr_metric_view_payload(
    *,
    view: MetricView,
    query: MetricQuery,
) -> dict[str, Any]:
    scope_key = _build_scope_key(query)
    if view == "player-seasons":
        return _build_rawr_player_seasons_payload(scope_key=scope_key, query=query)
    if view == "span-chart":
        return _build_metric_span_chart_payload(
            metric=Metric.RAWR,
            scope_key=scope_key,
            top_n=query.top_n,
        )
    if view == "cached-leaderboard":
        return _build_rawr_cached_leaderboard_payload(scope_key=scope_key, query=query)
    if view == "custom-query":
        return _build_rawr_custom_leaderboard_payload(query=query)
    raise ValueError(f"Unknown metric view: {view}")


def _build_wowy_metric_view_payload(
    *,
    metric: Metric,
    view: MetricView,
    query: MetricQuery,
) -> dict[str, Any]:
    scope_key = _build_scope_key(query)
    if view == "player-seasons":
        return _build_wowy_player_seasons_payload(metric=metric, scope_key=scope_key, query=query)
    if view == "span-chart":
        return _build_metric_span_chart_payload(
            metric=metric,
            scope_key=scope_key,
            top_n=query.top_n,
        )
    if view == "cached-leaderboard":
        return _build_wowy_cached_leaderboard_payload(
            metric=metric,
            scope_key=scope_key,
            query=query,
        )
    if view == "custom-query":
        return _build_wowy_custom_leaderboard_payload(metric=metric, query=query)
    raise ValueError(f"Unknown metric view: {view}")


def _build_rawr_metric_export_table(
    *,
    view: MetricView,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    scope_key = _build_scope_key(query)
    if view == "cached-leaderboard":
        catalog_row = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
        rows = load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games=query.min_games,
        )
        return (
            _metric_label(Metric.RAWR),
            _build_rawr_ranked_table_rows(
                rows=rows,
                seasons=season_ids(query.seasons) or catalog_row.available_season_ids,
                top_n=None,
            ),
        )
    if view == "custom-query":
        result = _build_rawr_custom_query(query=query)
        seasons_in_scope = sorted({row.season_id for row in result.rows})
        return (
            result.metric_label,
            _build_rawr_ranked_table_rows(
                rows=result.rows,
                seasons=seasons_in_scope,
                top_n=None,
            ),
        )
    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_wowy_metric_export_table(
    *,
    metric: Metric,
    view: MetricView,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    scope_key = _build_scope_key(query)
    if view == "cached-leaderboard":
        catalog_row = require_current_metric_scope(metric=metric, scope_key=scope_key)
        rows = load_wowy_player_season_value_rows(
            metric_id=metric.value,
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
            min_games_with=query.min_games_with,
            min_games_without=query.min_games_without,
        )
        return (
            _metric_label(metric),
            _build_wowy_ranked_table_rows(
                rows=rows,
                seasons=season_ids(query.seasons) or catalog_row.available_season_ids,
                top_n=None,
            ),
        )
    if view == "custom-query":
        result = _build_wowy_custom_query(metric=metric, query=query)
        seasons_in_scope = sorted({row.season_id for row in result.rows})
        return (
            result.metric_label,
            _build_wowy_ranked_table_rows(
                rows=result.rows,
                seasons=seasons_in_scope,
                top_n=None,
            ),
        )
    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_rawr_player_seasons_payload(
    *,
    scope_key: str,
    query: MetricQuery,
) -> dict[str, Any]:
    require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
    rows = load_rawr_player_season_value_rows(
        scope_key=scope_key,
        seasons=season_ids(query.seasons),
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        min_games=query.min_games,
    )
    return {
        "metric": Metric.RAWR.value,
        "metric_label": _metric_label(Metric.RAWR),
        "rows": [_serialize_rawr_player_season_row(row) for row in rows],
    }


def _build_wowy_player_seasons_payload(
    *,
    metric: Metric,
    scope_key: str,
    query: MetricQuery,
) -> dict[str, Any]:
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
    return {
        "metric": metric.value,
        "metric_label": _metric_label(metric),
        "rows": [_serialize_wowy_player_season_row(row) for row in rows],
    }


def _build_rawr_cached_leaderboard_payload(
    *,
    scope_key: str,
    query: MetricQuery,
) -> dict[str, Any]:
    catalog_row = require_current_metric_scope(metric=Metric.RAWR, scope_key=scope_key)
    rows = load_rawr_player_season_value_rows(
        scope_key=scope_key,
        seasons=season_ids(query.seasons),
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        min_games=query.min_games,
    )
    selected_season_ids = season_ids(query.seasons) or catalog_row.available_season_ids
    payload = _build_rawr_leaderboard_payload(
        metric=Metric.RAWR.value,
        metric_label=_metric_label(Metric.RAWR),
        rows=rows,
        seasons=selected_season_ids,
        top_n=query.top_n,
        mode="cached",
    )
    payload["available_seasons"] = _build_available_seasons(catalog_row)
    payload["available_teams"] = _build_available_teams(catalog_row)
    return payload


def _build_wowy_cached_leaderboard_payload(
    *,
    metric: Metric,
    scope_key: str,
    query: MetricQuery,
) -> dict[str, Any]:
    catalog_row = require_current_metric_scope(metric=metric, scope_key=scope_key)
    rows = load_wowy_player_season_value_rows(
        metric_id=metric.value,
        scope_key=scope_key,
        seasons=season_ids(query.seasons),
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        min_games_with=query.min_games_with,
        min_games_without=query.min_games_without,
    )
    selected_season_ids = season_ids(query.seasons) or catalog_row.available_season_ids
    payload = _build_wowy_leaderboard_payload(
        metric=metric.value,
        metric_label=_metric_label(metric),
        rows=rows,
        seasons=selected_season_ids,
        top_n=query.top_n,
        mode="cached",
    )
    payload["available_seasons"] = _build_available_seasons(catalog_row)
    payload["available_teams"] = _build_available_teams(catalog_row)
    return payload


def _build_rawr_custom_leaderboard_payload(
    *,
    query: MetricQuery,
) -> dict[str, Any]:
    result = _build_rawr_custom_query(query=query)
    seasons_in_scope = sorted({row.season_id for row in result.rows})
    return _build_rawr_leaderboard_payload(
        metric=result.metric,
        metric_label=result.metric_label,
        rows=result.rows,
        seasons=seasons_in_scope,
        top_n=query.top_n,
        mode="custom",
    )


def _build_wowy_custom_leaderboard_payload(
    *,
    metric: Metric,
    query: MetricQuery,
) -> dict[str, Any]:
    result = _build_wowy_custom_query(metric=metric, query=query)
    seasons_in_scope = sorted({row.season_id for row in result.rows})
    return _build_wowy_leaderboard_payload(
        metric=result.metric,
        metric_label=result.metric_label,
        rows=result.rows,
        seasons=seasons_in_scope,
        top_n=query.top_n,
        mode="custom",
    )


def _build_rawr_custom_query(
    *,
    query: MetricQuery,
) -> RawrCustomQueryResult:
    return build_rawr_custom_query(
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
        min_games=int(query.min_games or 0),
        ridge_alpha=float(query.ridge_alpha or _rawr_default_filters()["ridge_alpha"]),
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
    )


def _build_wowy_custom_query(
    *,
    metric: Metric,
    query: MetricQuery,
) -> WowyCustomQueryResult:
    return build_wowy_custom_query(
        metric,
        teams=query.teams,
        seasons=query.seasons,
        season_type=query.season_type,
        min_games_with=int(query.min_games_with or 0),
        min_games_without=int(query.min_games_without or 0),
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
    )


def _serialize_rawr_player_season_row(
    row: RawrPlayerSeasonValueRow,
) -> dict[str, Any]:
    return {
        "season_id": row.season_id,
        "player_id": row.player_id,
        "player_name": row.player_name,
        "value": row.coefficient,
        "sample_size": row.games,
        "secondary_sample_size": None,
        "games": row.games,
        "average_minutes": row.average_minutes,
        "total_minutes": row.total_minutes,
    }


def _serialize_wowy_player_season_row(
    row: WowyPlayerSeasonValueRow,
) -> dict[str, Any]:
    return {
        "season_id": row.season_id,
        "player_id": row.player_id,
        "player_name": row.player_name,
        "value": row.value,
        "sample_size": row.games_with,
        "secondary_sample_size": row.games_without,
        "games_with": row.games_with,
        "games_without": row.games_without,
        "avg_margin_with": row.avg_margin_with,
        "avg_margin_without": row.avg_margin_without,
        "average_minutes": row.average_minutes,
        "total_minutes": row.total_minutes,
        "raw_wowy_score": row.raw_wowy_score,
    }


def _build_metric_span_chart_payload(
    metric: Metric,
    *,
    scope_key: str,
    top_n: int,
) -> dict[str, Any]:
    catalog_row = require_current_metric_scope(metric=metric, scope_key=scope_key)
    span_rows = load_metric_span_store_rows(
        metric=metric.value,
        scope_key=scope_key,
        top_n=top_n,
    )
    return {
        "metric": metric.value,
        "metric_label": catalog_row.label,
        "span": {
            "start_season": catalog_row.full_span_start_season_id,
            "end_season": catalog_row.full_span_end_season_id,
            "available_seasons": catalog_row.available_season_ids,
            "top_n": top_n,
        },
        "series": _build_metric_span_series(
            catalog_row=catalog_row,
            top_n=top_n,
            points_map=span_rows.points_map,
            series_rows=span_rows.series_rows,
        ),
    }


def _metric_label(metric: Metric) -> str:
    if metric == Metric.RAWR:
        return describe_rawr_metric().label
    return describe_wowy_metric(metric).label


def _build_rawr_leaderboard_payload(
    *,
    metric: str,
    metric_label: str,
    rows: list[RawrPlayerSeasonValueRow] | list[RawrCustomQueryRow],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    table_rows = _build_rawr_ranked_table_rows(rows=rows, seasons=seasons, top_n=top_n)
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": _build_span_payload(seasons=seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": _build_series_from_table_rows(table_rows),
    }


def _build_wowy_leaderboard_payload(
    *,
    metric: str,
    metric_label: str,
    rows: list[WowyPlayerSeasonValueRow] | list[WowyCustomQueryRow],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    table_rows = _build_wowy_ranked_table_rows(rows=rows, seasons=seasons, top_n=top_n)
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": _build_span_payload(seasons=seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": _build_series_from_table_rows(table_rows),
    }


def _build_rawr_ranked_table_rows(
    *,
    rows: list[RawrPlayerSeasonValueRow] | list[RawrCustomQueryRow],
    seasons: list[str],
    top_n: int | None,
) -> list[dict[str, Any]]:
    rows_by_player: dict[int, list[RawrPlayerSeasonValueRow | RawrCustomQueryRow]] = {}
    for row in rows:
        rows_by_player.setdefault(row.player_id, []).append(row)

    ordered_seasons = sorted(dict.fromkeys(seasons))
    full_span_length = len(ordered_seasons) or 1
    ranked_rows: list[dict[str, Any]] = []
    for player_id, player_rows in rows_by_player.items():
        total_minutes = sum((row.total_minutes or 0.0) for row in player_rows)
        games = sum(row.games for row in player_rows)
        average_minutes = total_minutes / games if games > 0 else None
        ranked_rows.append(
            {
                "rank": 0,
                "player_id": player_id,
                "player_name": player_rows[0].player_name,
                "span_average_value": (
                    sum(_rawr_row_value(row) for row in player_rows) / full_span_length
                ),
                "average_minutes": average_minutes,
                "total_minutes": total_minutes,
                "games_with": games,
                "games_without": 0,
                "avg_margin_with": None,
                "avg_margin_without": None,
                "season_count": len(player_rows),
                "points": [
                    {
                        "season": season_id,
                        "value": next(
                            (
                                _rawr_row_value(row)
                                for row in player_rows
                                if row.season_id == season_id
                            ),
                            None,
                        ),
                    }
                    for season_id in ordered_seasons
                ],
            }
        )

    ranked_rows.sort(
        key=lambda row: (row["span_average_value"], row["player_name"]),
        reverse=True,
    )
    limited_rows = ranked_rows if top_n is None else ranked_rows[:top_n]
    return [{**row, "rank": index + 1} for index, row in enumerate(limited_rows)]


def _build_wowy_ranked_table_rows(
    *,
    rows: list[WowyPlayerSeasonValueRow] | list[WowyCustomQueryRow],
    seasons: list[str],
    top_n: int | None,
) -> list[dict[str, Any]]:
    rows_by_player: dict[int, list[WowyPlayerSeasonValueRow | WowyCustomQueryRow]] = {}
    for row in rows:
        rows_by_player.setdefault(row.player_id, []).append(row)

    ordered_seasons = sorted(dict.fromkeys(seasons))
    full_span_length = len(ordered_seasons) or 1
    ranked_rows: list[dict[str, Any]] = []
    for player_id, player_rows in rows_by_player.items():
        total_minutes = sum((row.total_minutes or 0.0) for row in player_rows)
        games_with = sum(row.games_with for row in player_rows)
        games_without = sum(row.games_without for row in player_rows)
        average_minutes = total_minutes / games_with if games_with > 0 else None
        ranked_rows.append(
            {
                "rank": 0,
                "player_id": player_id,
                "player_name": player_rows[0].player_name,
                "span_average_value": sum(row.value for row in player_rows) / full_span_length,
                "average_minutes": average_minutes,
                "total_minutes": total_minutes,
                "games_with": games_with,
                "games_without": games_without,
                "avg_margin_with": _weighted_average_wowy_rows(
                    player_rows,
                    value_key="avg_margin_with",
                    weight_key="games_with",
                ),
                "avg_margin_without": _weighted_average_wowy_rows(
                    player_rows,
                    value_key="avg_margin_without",
                    weight_key="games_without",
                ),
                "season_count": len(player_rows),
                "points": [
                    {
                        "season": season_id,
                        "value": next(
                            (row.value for row in player_rows if row.season_id == season_id),
                            None,
                        ),
                    }
                    for season_id in ordered_seasons
                ],
            }
        )

    ranked_rows.sort(
        key=lambda row: (row["span_average_value"], row["player_name"]),
        reverse=True,
    )
    limited_rows = ranked_rows if top_n is None else ranked_rows[:top_n]
    return [{**row, "rank": index + 1} for index, row in enumerate(limited_rows)]


def _build_span_payload(
    *,
    seasons: list[str],
    top_n: int,
) -> dict[str, Any]:
    ordered_seasons = sorted(dict.fromkeys(seasons))
    return {
        "start_season": ordered_seasons[0] if ordered_seasons else None,
        "end_season": ordered_seasons[-1] if ordered_seasons else None,
        "available_seasons": ordered_seasons,
        "top_n": top_n,
    }


def _build_series_from_table_rows(
    table_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "span_average_value": row["span_average_value"],
            "season_count": row["season_count"],
            "points": row["points"],
        }
        for row in table_rows
    ]


def _build_scope_key(query: MetricQuery) -> str:
    team_filter = build_team_filter(query.teams)
    return build_scope_key(season_type=query.season_type, team_filter=team_filter)


def _build_available_seasons(catalog_row: MetricScopeCatalogRow) -> list[Season]:
    resolved_season_type = SeasonType.parse(catalog_row.season_type)
    return [
        Season(season_id, resolved_season_type.to_nba_format())
        for season_id in catalog_row.available_season_ids
    ]


def _build_available_teams(catalog_row: MetricScopeCatalogRow) -> list[Team]:
    return [Team.from_id(team_id) for team_id in catalog_row.available_team_ids]


def _build_metric_span_series(
    *,
    catalog_row: MetricScopeCatalogRow,
    series_rows,
    points_map: dict[int, dict[str, float]],
    top_n: int,
) -> list[dict[str, Any]]:
    return [
        {
            "player_id": row.player_id,
            "player_name": row.player_name,
            "span_average_value": row.span_average_value,
            "season_count": row.season_count,
            "points": [
                {
                    "season": season,
                    "value": points_map.get(row.player_id, {}).get(season),
                }
                for season in catalog_row.available_season_ids
            ],
        }
        for row in series_rows[:top_n]
    ]


def _rawr_row_value(row: RawrPlayerSeasonValueRow | RawrCustomQueryRow) -> float:
    if isinstance(row, RawrPlayerSeasonValueRow):
        return row.coefficient
    return row.coefficient


def _weighted_average_wowy_rows(
    rows: list[WowyPlayerSeasonValueRow | WowyCustomQueryRow],
    *,
    value_key: str,
    weight_key: str,
) -> float | None:
    weighted_total = 0.0
    weight_total = 0
    for row in rows:
        value = getattr(row, value_key)
        weight = getattr(row, weight_key)
        if value is None or weight <= 0:
            continue
        weighted_total += value * weight
        weight_total += weight
    if weight_total == 0:
        return None
    return weighted_total / weight_total
