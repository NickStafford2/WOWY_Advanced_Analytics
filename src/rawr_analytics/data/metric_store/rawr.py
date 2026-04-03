from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store.schema import connect, initialize_player_metrics_db
from rawr_analytics.metrics.rawr import RawrPlayerSeasonValue, RawrValue
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary


@dataclass(frozen=True)
class RawrPlayerSeasonValueRow:
    snapshot_id: int | None
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    value: RawrPlayerSeasonValue


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
            snapshot.snapshot_id,
            snapshot.metric_id,
            snapshot.scope_key,
            rawr.team_filter,
            rawr.season_type,
            rawr.season_id,
            rawr.player_id,
            rawr.player_name,
            rawr.coefficient,
            rawr.games,
            rawr.average_minutes,
            rawr.total_minutes
        FROM rawr_player_season_values AS rawr
        INNER JOIN metric_snapshot AS snapshot
            ON snapshot.snapshot_id = rawr.snapshot_id
        WHERE snapshot.metric_id = 'rawr' AND snapshot.scope_key = ?
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
    with connect(METRIC_STORE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        RawrPlayerSeasonValueRow(
            snapshot_id=row["snapshot_id"],
            metric_id=row["metric_id"],
            scope_key=row["scope_key"],
            team_filter=row["team_filter"],
            season_type=row["season_type"],
            value=RawrPlayerSeasonValue(
                season_id=row["season_id"],
                player=PlayerSummary(
                    player_id=row["player_id"],
                    player_name=row["player_name"],
                ),
                minutes=PlayerMinutes(
                    average_minutes=row["average_minutes"],
                    total_minutes=row["total_minutes"],
                ),
                result=RawrValue(
                    games=row["games"],
                    coefficient=row["coefficient"],
                ),
            ),
        )
        for row in rows
    ]
