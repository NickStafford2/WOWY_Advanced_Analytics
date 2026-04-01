from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store.schema import connect, initialize_player_metrics_db


@dataclass(frozen=True)
class WowyPlayerSeasonValueRow:
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    season_id: str
    player_id: int
    player_name: str
    value: float
    games_with: int
    games_without: int
    avg_margin_with: float
    avg_margin_without: float
    average_minutes: float | None
    total_minutes: float | None
    raw_wowy_score: float | None = None


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
    initialize_player_metrics_db()
    query = """
        SELECT
            metric_id,
            scope_key,
            team_filter,
            season_type,
            season_id,
            player_id,
            player_name,
            value,
            games_with,
            games_without,
            avg_margin_with,
            avg_margin_without,
            average_minutes,
            total_minutes,
            raw_wowy_score
        FROM wowy_player_season_values
        WHERE metric_id = ? AND scope_key = ?
    """
    params: list[object] = [metric_id, scope_key]
    if seasons:
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
    return [
        WowyPlayerSeasonValueRow(
            metric_id=row["metric_id"],
            scope_key=row["scope_key"],
            team_filter=row["team_filter"],
            season_type=row["season_type"],
            season_id=row["season_id"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            value=row["value"],
            games_with=row["games_with"],
            games_without=row["games_without"],
            avg_margin_with=row["avg_margin_with"],
            avg_margin_without=row["avg_margin_without"],
            average_minutes=row["average_minutes"],
            total_minutes=row["total_minutes"],
            raw_wowy_score=row["raw_wowy_score"],
        )
        for row in rows
    ]
