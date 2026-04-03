from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from rawr_analytics.data.metric_store import WowyPlayerSeasonValueRow
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.defaults import describe_metric
from rawr_analytics.metrics.wowy.models import WowyCustomQueryResult, WowyCustomQueryRow
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

type WowyQueryRow = WowyPlayerSeasonValueRow | WowyCustomQueryRow


@dataclass(frozen=True)
class WowyQueryFilters:
    teams: list[Team] | None
    seasons: list[Season] | None
    season_type: SeasonType
    min_average_minutes: float
    min_total_minutes: float
    top_n: int
    min_games_with: int
    min_games_without: int


def build_query_filters_payload(
    *,
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType,
    min_average_minutes: float,
    min_total_minutes: float,
    top_n: int,
    min_games_with: int | None,
    min_games_without: int | None,
) -> WowyQueryFilters:
    assert min_games_with is not None
    assert min_games_without is not None
    return WowyQueryFilters(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        top_n=top_n,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
    )


def build_options_filters_payload(filters: WowyQueryFilters) -> WowyQueryFilters:
    return WowyQueryFilters(
        teams=filters.teams,
        seasons=None,
        season_type=filters.season_type,
        min_average_minutes=filters.min_average_minutes,
        min_total_minutes=filters.min_total_minutes,
        top_n=filters.top_n,
        min_games_with=filters.min_games_with,
        min_games_without=filters.min_games_without,
    )


def build_player_seasons_payload(
    metric: Metric,
    rows: Sequence[WowyPlayerSeasonValueRow],
) -> dict[str, Any]:
    return {
        "metric": metric.value,
        "metric_label": describe_metric(metric).label,
        "rows": [_serialize_player_season_row(row) for row in rows],
    }


def build_cached_leaderboard_payload(
    metric: Metric,
    *,
    metric_label: str,
    available_seasons: list[Season],
    available_teams: list[Team],
    rows: Sequence[WowyPlayerSeasonValueRow],
    seasons: list[str],
    top_n: int,
) -> dict[str, Any]:
    payload = _build_leaderboard_payload(
        metric=metric.value,
        metric_label=metric_label,
        rows=rows,
        seasons=seasons,
        top_n=top_n,
        mode="cached",
    )
    payload["available_seasons"] = available_seasons
    payload["available_teams"] = available_teams
    return payload


def build_custom_leaderboard_payload(
    metric: Metric,
    result: WowyCustomQueryResult,
    *,
    top_n: int,
) -> dict[str, Any]:
    return _build_leaderboard_payload(
        metric=metric.value,
        metric_label=result.metric_label,
        rows=result.rows,
        seasons=sorted({row.season_id for row in result.rows}),
        top_n=top_n,
        mode="custom",
    )


def build_export_table(
    metric: Metric,
    *,
    rows: Sequence[WowyQueryRow],
    seasons: list[str],
    metric_label: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    return (
        metric_label or describe_metric(metric).label,
        _build_ranked_table_rows(rows=rows, seasons=seasons, top_n=None),
    )


def _serialize_player_season_row(
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


def _build_leaderboard_payload(
    *,
    metric: str,
    metric_label: str,
    rows: Sequence[WowyQueryRow],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    table_rows = _build_ranked_table_rows(rows=rows, seasons=seasons, top_n=top_n)
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": _build_span_payload(seasons=seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": _build_series_from_table_rows(table_rows),
    }


def _build_ranked_table_rows(
    *,
    rows: Sequence[WowyQueryRow],
    seasons: list[str],
    top_n: int | None,
) -> list[dict[str, Any]]:
    rows_by_player: dict[int, list[WowyQueryRow]] = {}
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
                "avg_margin_with": _weighted_average_rows(
                    player_rows,
                    value_key="avg_margin_with",
                    weight_key="games_with",
                ),
                "avg_margin_without": _weighted_average_rows(
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


def _weighted_average_rows(
    rows: Sequence[WowyQueryRow],
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
