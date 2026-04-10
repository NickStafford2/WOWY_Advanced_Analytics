from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import cast

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store.schema import connect, initialize_player_metrics_db


@dataclass(frozen=True)
class WowyPlayerSeasonValueRow:
    snapshot_id: int | None
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    season_id: str
    player_id: int
    player_name: str
    value: float | None
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    average_minutes: float | None
    total_minutes: float | None
    raw_wowy_score: float | None


def load_wowy_player_season_value_rows(
    *,
    metric_id: str,
    scope_key: str,
    seasons: list[str] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
) -> list[WowyPlayerSeasonValueRow]:
    if seasons == []:
        return []
    initialize_player_metrics_db()
    query = """
        SELECT
            snapshot.snapshot_id,
            snapshot.metric_id,
            snapshot.scope_key,
            wowy.team_filter,
            wowy.season_type,
            wowy.season_id,
            wowy.player_id,
            wowy.player_name,
            wowy.value,
            wowy.games_with,
            wowy.games_without,
            wowy.avg_margin_with,
            wowy.avg_margin_without,
            wowy.average_minutes,
            wowy.total_minutes,
            wowy.raw_wowy_score
        FROM wowy_player_season_values AS wowy
        INNER JOIN metric_snapshot AS snapshot
            ON snapshot.snapshot_id = wowy.snapshot_id
        WHERE snapshot.metric_id = ? AND snapshot.scope_key = ?
    """
    params: list[object] = [metric_id, scope_key]
    if seasons is not None:
        query += f" AND season_id IN ({','.join('?' for _ in seasons)})"
        params.extend(seasons)
    if min_average_minutes is not None:
        query += " AND COALESCE(average_minutes, 0.0) >= ?"
        params.append(min_average_minutes)
    if min_total_minutes is not None:
        query += " AND COALESCE(total_minutes, 0.0) >= ?"
        params.append(min_total_minutes)
    if min_games_with is not None:
        query += " AND games_with >= ?"
        params.append(min_games_with)
    if min_games_without is not None:
        query += " AND games_without >= ?"
        params.append(min_games_without)
    query += " ORDER BY season_id, value DESC, player_name ASC"
    with connect(METRIC_STORE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [_build_wowy_player_season_value_row(row) for row in rows]


def _build_wowy_player_season_value_row(row: sqlite3.Row) -> WowyPlayerSeasonValueRow:
    return WowyPlayerSeasonValueRow(
        snapshot_id=cast(int | None, row["snapshot_id"]),
        metric_id=cast(str, row["metric_id"]),
        scope_key=cast(str, row["scope_key"]),
        team_filter=cast(str, row["team_filter"]),
        season_type=cast(str, row["season_type"]),
        season_id=cast(str, row["season_id"]),
        player_id=cast(int, row["player_id"]),
        player_name=cast(str, row["player_name"]),
        value=cast(float | None, row["value"]),
        games_with=cast(int, row["games_with"]),
        games_without=cast(int, row["games_without"]),
        avg_margin_with=cast(float | None, row["avg_margin_with"]),
        avg_margin_without=cast(float | None, row["avg_margin_without"]),
        average_minutes=cast(float | None, row["average_minutes"]),
        total_minutes=cast(float | None, row["total_minutes"]),
        raw_wowy_score=cast(float | None, row["raw_wowy_score"]),
    )
