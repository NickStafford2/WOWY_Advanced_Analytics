from __future__ import annotations

from typing import Any

from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter, season_ids
from rawr_analytics.data.metric_store_views import (
    CachedMetricRow,
    load_cached_metric_leaderboard_snapshot,
    load_cached_metric_player_seasons_snapshot,
    load_cached_metric_span_snapshot,
)
from rawr_analytics.data.player_metrics_db.rawr import RawrPlayerSeasonValueRow
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import (
    build_rawr_custom_query,
)
from rawr_analytics.metrics.rawr import (
    default_filters as _rawr_default_filters,
)
from rawr_analytics.metrics.rawr.models import RawrCustomQueryResult, RawrCustomQueryRow
from rawr_analytics.metrics.wowy import build_wowy_custom_query
from rawr_analytics.metrics.wowy.models import WowyCustomQueryResult, WowyCustomQueryRow

from .models import MetricQuery
from .scope import build_filters_payload

MetricView = str


def build_metric_view_payload(
    metric: Metric,
    *,
    view: MetricView,
    query: MetricQuery,
) -> dict[str, Any]:
    team_filter = build_team_filter(query.teams)
    scope_key = build_scope_key(season_type=query.season_type, team_filter=team_filter)
    if view == "player-seasons":
        payload = _build_metric_player_seasons_payload(metric, scope_key=scope_key, query=query)
    elif view == "span-chart":
        payload = _build_metric_span_chart_payload(metric, scope_key=scope_key, top_n=query.top_n)
    elif view == "cached-leaderboard":
        payload = _build_cached_metric_leaderboard_payload(metric, scope_key=scope_key, query=query)
    elif view == "custom-query":
        payload = _build_custom_metric_leaderboard_payload(metric, query=query)
    else:
        raise ValueError(f"Unknown metric view: {view}")
    payload["filters"] = build_filters_payload(query)
    return payload


def build_metric_export_table(
    metric: Metric,
    *,
    view: MetricView,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    team_filter = build_team_filter(query.teams)
    scope_key = build_scope_key(season_type=query.season_type, team_filter=team_filter)
    if view == "cached-leaderboard":
        return _build_cached_metric_export_table_rows(metric, scope_key=scope_key, query=query)
    if view == "custom-query":
        return _build_custom_metric_export_table_rows(metric, query=query)
    raise ValueError(f"Metric view {view!r} does not support CSV export")


def _build_metric_player_seasons_payload(
    metric: Metric,
    *,
    scope_key: str,
    query: MetricQuery,
) -> dict[str, Any]:
    snapshot = load_cached_metric_player_seasons_snapshot(
        metric=metric,
        scope_key=scope_key,
        seasons=season_ids(query.seasons),
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        min_sample_size=_metric_min_sample_size(query),
        min_secondary_sample_size=_metric_secondary_sample_size(query),
    )
    return {
        "metric": snapshot.metric,
        "metric_label": snapshot.metric_label,
        "rows": [_serialize_cached_row(row) for row in snapshot.rows],
    }


def _build_cached_metric_leaderboard_payload(
    metric: Metric,
    *,
    scope_key: str,
    query: MetricQuery,
) -> dict[str, Any]:
    snapshot = load_cached_metric_leaderboard_snapshot(
        metric=metric,
        scope_key=scope_key,
        seasons=season_ids(query.seasons),
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        min_sample_size=_metric_min_sample_size(query),
        min_secondary_sample_size=_metric_secondary_sample_size(query),
    )
    payload = _build_leaderboard_payload_from_rows(
        metric=snapshot.metric,
        metric_label=snapshot.metric_label,
        rows=[_serialize_cached_row(row) for row in snapshot.rows],
        seasons=snapshot.season_ids,
        top_n=query.top_n,
        mode="cached",
    )
    payload["available_seasons"] = snapshot.available_seasons
    payload["available_teams"] = snapshot.available_teams
    return payload


def _build_cached_metric_export_table_rows(
    metric: Metric,
    *,
    scope_key: str,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    snapshot = load_cached_metric_leaderboard_snapshot(
        metric=metric,
        scope_key=scope_key,
        seasons=season_ids(query.seasons),
        min_average_minutes=query.min_average_minutes,
        min_total_minutes=query.min_total_minutes,
        min_sample_size=_metric_min_sample_size(query),
        min_secondary_sample_size=_metric_secondary_sample_size(query),
    )
    table_rows = _build_ranked_table_rows(
        [_serialize_cached_row(row) for row in snapshot.rows],
        seasons=snapshot.season_ids,
        top_n=None,
    )
    return snapshot.metric_label, table_rows


def _build_custom_metric_leaderboard_payload(
    metric: Metric,
    *,
    query: MetricQuery,
) -> dict[str, Any]:
    custom_query = _build_custom_metric_query(metric, query=query)
    rows = _serialize_custom_query_rows(custom_query)
    seasons_in_scope = sorted({row["season_id"] for row in rows})
    return _build_leaderboard_payload_from_custom_rows(
        metric=custom_query.metric,
        metric_label=custom_query.metric_label,
        rows=rows,
        seasons=seasons_in_scope,
        top_n=query.top_n,
        mode="custom",
    )


def _build_custom_metric_export_table_rows(
    metric: Metric,
    *,
    query: MetricQuery,
) -> tuple[str, list[dict[str, Any]]]:
    custom_query = _build_custom_metric_query(metric, query=query)
    rows = _serialize_custom_query_rows(custom_query)
    seasons_in_scope = sorted({row["season_id"] for row in rows})
    return (
        custom_query.metric_label,
        _build_ranked_table_rows(rows, seasons=seasons_in_scope, top_n=None),
    )


def _build_custom_metric_query(
    metric: Metric,
    *,
    query: MetricQuery,
) -> RawrCustomQueryResult | WowyCustomQueryResult:
    if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
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
    if metric == Metric.RAWR:
        return build_rawr_custom_query(
            teams=query.teams,
            seasons=query.seasons,
            season_type=query.season_type,
            min_games=int(query.min_games or 0),
            ridge_alpha=float(query.ridge_alpha or _rawr_default_filters()["ridge_alpha"]),
            min_average_minutes=query.min_average_minutes,
            min_total_minutes=query.min_total_minutes,
        )
    raise ValueError(f"Unknown metric: {metric}")


def _serialize_cached_row(row: CachedMetricRow) -> dict[str, Any]:
    if isinstance(row, RawrPlayerSeasonValueRow):
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


def _serialize_custom_query_rows(
    result: RawrCustomQueryResult | WowyCustomQueryResult,
) -> list[dict[str, Any]]:
    if isinstance(result, RawrCustomQueryResult):
        return [_serialize_rawr_custom_query_row(row) for row in result.rows]
    return [_serialize_wowy_custom_query_row(row) for row in result.rows]


def _serialize_rawr_custom_query_row(row: RawrCustomQueryRow) -> dict[str, Any]:
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


def _serialize_wowy_custom_query_row(row: WowyCustomQueryRow) -> dict[str, Any]:
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
    snapshot = load_cached_metric_span_snapshot(metric=metric, scope_key=scope_key, top_n=top_n)
    return {
        "metric": snapshot.metric,
        "metric_label": snapshot.metric_label,
        "span": {
            "start_season": snapshot.start_season,
            "end_season": snapshot.end_season,
            "available_seasons": snapshot.available_seasons,
            "top_n": snapshot.top_n,
        },
        "series": snapshot.series,
    }


def _metric_min_sample_size(query: MetricQuery) -> int | None:
    return query.min_games_with if query.min_games_with is not None else query.min_games


def _metric_secondary_sample_size(query: MetricQuery) -> int | None:
    return query.min_games_without


def _build_leaderboard_payload_from_rows(
    *,
    metric: str,
    metric_label: str,
    rows: list[dict[str, Any]],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    table_rows = _build_ranked_table_rows(
        rows,
        seasons=seasons,
        top_n=top_n,
    )
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": _build_span_payload(seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": _build_series_from_table_rows(table_rows, seasons=seasons),
    }


def _build_leaderboard_payload_from_custom_rows(
    *,
    metric: str,
    metric_label: str,
    rows: list[dict[str, Any]],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    table_rows = _build_ranked_table_rows(rows, seasons=seasons, top_n=top_n)
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": _build_span_payload(seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": _build_series_from_table_rows(table_rows, seasons=seasons),
    }


def _build_span_payload(seasons: list[str], *, top_n: int) -> dict[str, Any]:
    ordered_seasons = sorted(dict.fromkeys(seasons))
    return {
        "start_season": ordered_seasons[0] if ordered_seasons else None,
        "end_season": ordered_seasons[-1] if ordered_seasons else None,
        "available_seasons": ordered_seasons,
        "top_n": top_n,
    }


def _build_ranked_table_rows(
    rows: list[dict[str, Any]],
    *,
    seasons: list[str],
    top_n: int | None,
) -> list[dict[str, Any]]:
    rows_by_player: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_player.setdefault(row["player_id"], []).append(row)

    ordered_seasons = sorted(dict.fromkeys(seasons))
    full_span_length = len(ordered_seasons) or 1
    ranked_rows = []
    for player_id, player_rows in rows_by_player.items():
        player_name = player_rows[0]["player_name"]
        games_with = sum(
            (row.get("games_with") or row.get("sample_size") or 0) for row in player_rows
        )
        games_without = sum(
            (row.get("games_without") or row.get("secondary_sample_size") or 0)
            for row in player_rows
        )
        total_minutes = sum((row.get("total_minutes") or 0.0) for row in player_rows)
        average_minutes = total_minutes / games_with if games_with > 0 else None
        ranked_rows.append(
            {
                "rank": 0,
                "player_id": player_id,
                "player_name": player_name,
                "span_average_value": sum(row["value"] for row in player_rows) / full_span_length,
                "average_minutes": average_minutes,
                "total_minutes": total_minutes,
                "games_with": games_with,
                "games_without": games_without,
                "avg_margin_with": _weighted_average_rows(
                    player_rows,
                    value_key="avg_margin_with",
                    weight_keys=("games_with", "sample_size"),
                ),
                "avg_margin_without": _weighted_average_rows(
                    player_rows,
                    value_key="avg_margin_without",
                    weight_keys=("games_without", "secondary_sample_size"),
                ),
                "season_count": len(player_rows),
                "points": [
                    {
                        "season": season,
                        "value": next(
                            (row["value"] for row in player_rows if row["season_id"] == season),
                            None,
                        ),
                    }
                    for season in ordered_seasons
                ],
            }
        )

    ranked_rows.sort(
        key=lambda row: (row["span_average_value"], row["player_name"]),
        reverse=True,
    )
    limited_rows = ranked_rows if top_n is None else ranked_rows[:top_n]
    return [{**row, "rank": index + 1} for index, row in enumerate(limited_rows)]


def _build_series_from_table_rows(
    table_rows: list[dict[str, Any]],
    *,
    seasons: list[str],
) -> list[dict[str, Any]]:
    season_order = sorted(dict.fromkeys(seasons))
    return [
        {
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "span_average_value": row["span_average_value"],
            "season_count": row["season_count"],
            "points": row.get(
                "points",
                [{"season": season, "value": None} for season in season_order],
            ),
        }
        for row in table_rows
    ]


def _weighted_average_rows(
    rows: list[dict[str, Any]],
    *,
    value_key: str,
    weight_keys: tuple[str, str],
) -> float | None:
    weighted_total = 0.0
    weight_total = 0
    for row in rows:
        value = row.get(value_key)
        weight = row.get(weight_keys[0]) or row.get(weight_keys[1]) or 0
        if value is None or weight <= 0:
            continue
        weighted_total += value * weight
        weight_total += weight
    if weight_total == 0:
        return None
    return weighted_total / weight_total
