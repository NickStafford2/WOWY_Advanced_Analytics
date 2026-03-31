from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.constants import DB_PATH
from rawr_analytics.data.player_metrics_db.schema import connect, initialize_player_metrics_db
from rawr_analytics.metrics.rawr.models import RawrPlayerSeasonRecord
from rawr_analytics.shared.season import SeasonType


@dataclass(frozen=True)
class RawrPlayerSeasonValueRow:
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    season_id: str
    player_id: int
    player_name: str
    coefficient: float
    games: int
    average_minutes: float | None
    total_minutes: float | None


def build_rawr_player_season_value_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
    records: list[RawrPlayerSeasonRecord],
) -> list[RawrPlayerSeasonValueRow]:
    return [
        RawrPlayerSeasonValueRow(
            metric_id="rawr",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type.value,
            season_id=record.season.id,
            player_id=record.player_id,
            player_name=record.player_name,
            coefficient=record.coefficient,
            games=record.games,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
        )
        for record in records
    ]


def load_rawr_player_season_value_rows(
    *,
    scope_key: str,
    seasons: list[str] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games: int | None = None,
) -> list[RawrPlayerSeasonValueRow]:
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
            coefficient,
            games,
            average_minutes,
            total_minutes
        FROM rawr_player_season_values
        WHERE scope_key = ?
    """
    params: list[object] = [scope_key]
    if seasons:
        query += f" AND season_id IN ({','.join('?' for _ in seasons)})"
        params.extend(seasons)
    if min_average_minutes is not None:
        query += " AND COALESCE(average_minutes, 0.0) >= ?"
        params.append(min_average_minutes)
    if min_total_minutes is not None:
        query += " AND COALESCE(total_minutes, 0.0) >= ?"
        params.append(min_total_minutes)
    if min_games is not None:
        query += " AND games >= ?"
        params.append(min_games)
    query += " ORDER BY season_id, coefficient DESC, player_name ASC"
    with connect(DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        RawrPlayerSeasonValueRow(
            metric_id=row["metric_id"],
            scope_key=row["scope_key"],
            team_filter=row["team_filter"],
            season_type=row["season_type"],
            season_id=row["season_id"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            coefficient=row["coefficient"],
            games=row["games"],
            average_minutes=row["average_minutes"],
            total_minutes=row["total_minutes"],
        )
        for row in rows
    ]
