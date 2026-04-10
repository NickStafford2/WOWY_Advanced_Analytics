from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import cast

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store.schema import connect, initialize_player_metrics_db
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrPlayerSeasonValueRow:
    snapshot_id: int | None
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    season_id: str
    player_id: int
    player_name: str
    games: int
    coefficient: float
    average_minutes: float | None
    total_minutes: float | None


def load_rawr_player_season_value_rows(
    *,
    scope_key: str,
    seasons: list[str],
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games: int | None = None,
) -> list[RawrPlayerSeasonValueRow]:
    assert seasons, "RAWR metric store reads require explicit non-empty seasons"
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
    return [_build_rawr_player_season_value_row(row) for row in rows]


def replace_rawr_scope_snapshot(
    *,
    scope_key: str,
    label: str,
    team_filter: str,
    season_type: SeasonType,
    seasons: list[Season],
    available_teams: list[Team],
    build_version: str,
    source_fingerprint: str,
    rows: list[RawrPlayerSeasonValueRow],
) -> None:
    from datetime import UTC, datetime

    from rawr_analytics.data.metric_store._catalog import (
        build_metric_scope_catalog,
        build_metric_scope_catalog_row,
    )
    from rawr_analytics.data.metric_store._replace import replace_metric_scope_snapshot
    from rawr_analytics.data.metric_store._sql_writes import insert_rawr_rows
    from rawr_analytics.data.metric_store._validation import validate_rawr_rows
    from rawr_analytics.data.metric_store.full_span import (
        MetricStorePlayerSeasonValue,
        build_metric_full_span_rows,
    )

    catalog = build_metric_scope_catalog(
        label=label,
        team_filter=team_filter,
        season_type=season_type,
        seasons=seasons,
        available_teams=available_teams,
    )
    validate_rawr_rows(
        scope_key=scope_key,
        seasons=seasons,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    series_rows, point_rows = build_metric_full_span_rows(
        metric_id="rawr",
        scope_key=scope_key,
        season_ids=catalog.availability.season_ids,
        player_season_values=[
            MetricStorePlayerSeasonValue(
                player_id=row.player_id,
                player_name=row.player_name,
                season_id=row.season_id,
                value=row.coefficient,
            )
            for row in rows
        ],
    )
    replace_metric_scope_snapshot(
        metric_id="rawr",
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        catalog_row=build_metric_scope_catalog_row(
            metric_id="rawr",
            scope_key=scope_key,
            catalog=catalog,
            updated_at=datetime.now(UTC).isoformat(),
        ),
        series_rows=series_rows,
        point_rows=point_rows,
        insert_rows=lambda connection, snapshot_id: insert_rawr_rows(
            connection,
            rows,
            snapshot_id,
        ),
        row_count=len(rows),
    )


def _build_rawr_player_season_value_row(row: sqlite3.Row) -> RawrPlayerSeasonValueRow:
    return RawrPlayerSeasonValueRow(
        snapshot_id=cast(int | None, row["snapshot_id"]),
        metric_id=cast(str, row["metric_id"]),
        scope_key=cast(str, row["scope_key"]),
        team_filter=cast(str, row["team_filter"]),
        season_type=cast(str, row["season_type"]),
        season_id=cast(str, row["season_id"]),
        player_id=cast(int, row["player_id"]),
        player_name=cast(str, row["player_name"]),
        games=cast(int, row["games"]),
        coefficient=cast(float, row["coefficient"]),
        average_minutes=cast(float | None, row["average_minutes"]),
        total_minutes=cast(float | None, row["total_minutes"]),
    )
