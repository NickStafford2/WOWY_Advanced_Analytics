from __future__ import annotations

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    build_rawr_player_season_value_row,
)
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def load_rawr_player_season_value_rows(
    *,
    scope_key: str,
    seasons: list[str],
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_games: int | None = None,
) -> list[RawrPlayerSeasonValueRow]:
    assert seasons, "RAWR metric store reads require explicit non-empty seasons"
    initialize_metric_store_db()
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
    return [build_rawr_player_season_value_row(row) for row in rows]


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
    updated_at = datetime.now(UTC).isoformat()
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
        updated_at=updated_at,
        catalog_row=build_metric_scope_catalog_row(
            metric_id="rawr",
            scope_key=scope_key,
            catalog=catalog,
            updated_at=updated_at,
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
