from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import describe_metric as describe_rawr_metric
from rawr_analytics.metrics.rawr.models import RawrCustomQueryResult, RawrCustomQueryRow
from rawr_analytics.metrics.wowy import describe_metric as describe_wowy_metric
from rawr_analytics.metrics.wowy.models import WowyCustomQueryResult, WowyCustomQueryRow

from .scope import MetricStoreCatalog

MetricView = str


class _RawrRow(Protocol):
    season_id: str
    player_id: int
    player_name: str
    coefficient: float
    games: int
    average_minutes: float | None
    total_minutes: float | None


class _WowyRow(Protocol):
    season_id: str
    player_id: int
    player_name: str
    value: float
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    average_minutes: float | None
    total_minutes: float | None
    raw_wowy_score: float | None


class _MetricSpanSeriesRow(Protocol):
    player_id: int
    player_name: str
    span_average_value: float
    season_count: int


def build_metric_player_seasons_payload(
    metric: Metric,
    *,
    rows: Sequence[_RawrRow] | Sequence[_WowyRow],
) -> dict[str, Any]:
    if metric == Metric.RAWR:
        return {
            "metric": Metric.RAWR.value,
            "metric_label": _metric_label(Metric.RAWR),
            "rows": [_serialize_rawr_player_season_row(row) for row in rows],
        }
    if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        return {
            "metric": metric.value,
            "metric_label": _metric_label(metric),
            "rows": [_serialize_wowy_player_season_row(row) for row in rows],
        }
    raise ValueError(f"Unknown metric: {metric}")


def build_metric_cached_leaderboard_payload(
    metric: Metric,
    *,
    catalog: MetricStoreCatalog,
    rows: Sequence[_RawrRow] | Sequence[_WowyRow],
    seasons: list[str],
    top_n: int,
) -> dict[str, Any]:
    if metric == Metric.RAWR:
        payload = _build_rawr_leaderboard_payload(
            metric=Metric.RAWR.value,
            metric_label=catalog.metric_label,
            rows=rows,
            seasons=seasons,
            top_n=top_n,
            mode="cached",
        )
    elif metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        payload = _build_wowy_leaderboard_payload(
            metric=metric.value,
            metric_label=catalog.metric_label,
            rows=rows,
            seasons=seasons,
            top_n=top_n,
            mode="cached",
        )
    else:
        raise ValueError(f"Unknown metric: {metric}")
    payload["available_seasons"] = catalog.available_seasons
    payload["available_teams"] = catalog.available_teams
    return payload


def build_metric_custom_leaderboard_payload(
    metric: Metric,
    *,
    result: RawrCustomQueryResult | WowyCustomQueryResult,
    top_n: int,
) -> dict[str, Any]:
    seasons_in_scope = sorted({row.season_id for row in result.rows})
    if metric == Metric.RAWR:
        return _build_rawr_leaderboard_payload(
            metric=result.metric,
            metric_label=result.metric_label,
            rows=result.rows,
            seasons=seasons_in_scope,
            top_n=top_n,
            mode="custom",
        )
    if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        return _build_wowy_leaderboard_payload(
            metric=result.metric,
            metric_label=result.metric_label,
            rows=result.rows,
            seasons=seasons_in_scope,
            top_n=top_n,
            mode="custom",
        )
    raise ValueError(f"Unknown metric: {metric}")


def build_metric_span_chart_payload(
    metric: Metric,
    *,
    catalog: MetricStoreCatalog,
    series_rows: Sequence[_MetricSpanSeriesRow],
    points_map: dict[int, dict[str, float]],
    top_n: int,
) -> dict[str, Any]:
    return {
        "metric": metric.value,
        "metric_label": catalog.metric_label,
        "span": {
            "start_season": catalog.full_span_start_season_id,
            "end_season": catalog.full_span_end_season_id,
            "available_seasons": [season.id for season in catalog.available_seasons],
            "top_n": top_n,
        },
        "series": _build_metric_span_series(
            available_season_ids=[season.id for season in catalog.available_seasons],
            series_rows=series_rows,
            points_map=points_map,
            top_n=top_n,
        ),
    }


def build_metric_export_table(
    metric: Metric,
    *,
    rows: Sequence[_RawrRow]
    | Sequence[RawrCustomQueryRow]
    | Sequence[_WowyRow]
    | Sequence[WowyCustomQueryRow],
    seasons: list[str],
    metric_label: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    if metric == Metric.RAWR:
        return (
            metric_label or _metric_label(Metric.RAWR),
            _build_rawr_ranked_table_rows(
                rows=rows,
                seasons=seasons,
                top_n=None,
            ),
        )
    if metric in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        return (
            metric_label or _metric_label(metric),
            _build_wowy_ranked_table_rows(
                rows=rows,
                seasons=seasons,
                top_n=None,
            ),
        )
    raise ValueError(f"Unknown metric: {metric}")


def _serialize_rawr_player_season_row(
    row: _RawrRow,
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
    row: _WowyRow,
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


def _metric_label(metric: Metric) -> str:
    if metric == Metric.RAWR:
        return describe_rawr_metric().label
    return describe_wowy_metric(metric).label


def _build_rawr_leaderboard_payload(
    *,
    metric: str,
    metric_label: str,
    rows: Sequence[_RawrRow] | Sequence[RawrCustomQueryRow],
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
    rows: Sequence[_WowyRow] | Sequence[WowyCustomQueryRow],
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
    rows: Sequence[_RawrRow] | Sequence[RawrCustomQueryRow],
    seasons: list[str],
    top_n: int | None,
) -> list[dict[str, Any]]:
    rows_by_player: dict[int, list[_RawrRow | RawrCustomQueryRow]] = {}
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
                "span_average_value": sum(row.coefficient for row in player_rows)
                / full_span_length,
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
                                row.coefficient
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
    rows: Sequence[_WowyRow] | Sequence[WowyCustomQueryRow],
    seasons: list[str],
    top_n: int | None,
) -> list[dict[str, Any]]:
    rows_by_player: dict[int, list[_WowyRow | WowyCustomQueryRow]] = {}
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


def _build_metric_span_series(
    *,
    available_season_ids: list[str],
    series_rows: Sequence[_MetricSpanSeriesRow],
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
                for season in available_season_ids
            ],
        }
        for row in series_rows[:top_n]
    ]


def _weighted_average_wowy_rows(
    rows: Sequence[_WowyRow] | Sequence[WowyCustomQueryRow],
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
