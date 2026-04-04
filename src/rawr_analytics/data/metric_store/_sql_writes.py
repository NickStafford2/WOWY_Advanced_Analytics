from __future__ import annotations

import sqlite3

from rawr_analytics.data.metric_store._catalog import MetricScopeCatalogRow
from rawr_analytics.data.metric_store._tables import metric_values_table
from rawr_analytics.data.metric_store.full_span import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
)
from rawr_analytics.data.metric_store.rawr import RawrPlayerSeasonValueRow
from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow


def delete_metric_full_span_rows(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    scope_key: str,
) -> None:
    params = (metric_id, scope_key)
    connection.execute(
        """
        DELETE FROM metric_full_span_points
        WHERE snapshot_id IN (
            SELECT snapshot_id
            FROM metric_snapshot
            WHERE metric_id = ? AND scope_key = ?
        )
        """,
        params,
    )
    connection.execute(
        """
        DELETE FROM metric_full_span_series
        WHERE snapshot_id IN (
            SELECT snapshot_id
            FROM metric_snapshot
            WHERE metric_id = ? AND scope_key = ?
        )
        """,
        params,
    )


def delete_metric_value_rows(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    scope_key: str,
) -> None:
    table = metric_values_table(metric_id)
    connection.execute(
        f"""
        DELETE FROM {table}
        WHERE snapshot_id IN (
            SELECT snapshot_id
            FROM metric_snapshot
            WHERE metric_id = ? AND scope_key = ?
        )
        """,
        (metric_id, scope_key),
    )


def delete_metric_rows(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    scope_key: str,
) -> None:
    delete_metric_value_rows(connection, metric_id=metric_id, scope_key=scope_key)


def insert_metric_snapshot(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    row_count: int,
    updated_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO metric_snapshot (
            metric_id,
            scope_key,
            build_version,
            source_fingerprint,
            row_count,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            metric_id,
            scope_key,
            build_version,
            source_fingerprint,
            row_count,
            updated_at,
        ),
    )
    assert cursor.lastrowid is not None, "metric_snapshot insert must produce snapshot_id"
    return int(cursor.lastrowid)


def insert_rawr_rows(
    connection,
    rows: list[RawrPlayerSeasonValueRow],
    snapshot_id: int | None,
) -> None:
    if not rows:
        return
    assert snapshot_id is not None, "rawr snapshot writes require snapshot_id"
    connection.executemany(
        """
        INSERT INTO rawr_player_season_values (
            snapshot_id,
            team_filter,
            season_type,
            season_id,
            player_id,
            player_name,
            coefficient,
            games,
            average_minutes,
            total_minutes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                snapshot_id,
                row.team_filter,
                row.season_type,
                row.season_id,
                row.player_id,
                row.player_name,
                row.coefficient,
                row.games,
                row.average_minutes,
                row.total_minutes,
            )
            for row in rows
        ],
    )


def insert_wowy_rows(
    connection,
    rows: list[WowyPlayerSeasonValueRow],
    snapshot_id: int | None,
) -> None:
    if not rows:
        return
    assert snapshot_id is not None, "wowy snapshot writes require snapshot_id"
    connection.executemany(
        """
        INSERT INTO wowy_player_season_values (
            snapshot_id,
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                snapshot_id,
                row.team_filter,
                row.season_type,
                row.season_id,
                row.player_id,
                row.player_name,
                row.value,
                row.games_with,
                row.games_without,
                row.avg_margin_with,
                row.avg_margin_without,
                row.average_minutes,
                row.total_minutes,
                row.raw_wowy_score,
            )
            for row in rows
        ],
    )


def insert_metric_scope_teams(connection, row: MetricScopeCatalogRow) -> None:
    if not row.available_team_ids:
        return
    connection.executemany(
        """
        INSERT INTO metric_scope_team (
            metric_id,
            scope_key,
            team_id
        ) VALUES (?, ?, ?)
        """,
        [
            (
                row.metric_id,
                row.scope_key,
                team_id,
            )
            for team_id in row.available_team_ids
        ],
    )


def insert_metric_scope_seasons(connection, row: MetricScopeCatalogRow) -> None:
    if not row.available_season_ids:
        return
    connection.executemany(
        """
        INSERT INTO metric_scope_season (
            metric_id,
            scope_key,
            season_id
        ) VALUES (?, ?, ?)
        """,
        [
            (
                row.metric_id,
                row.scope_key,
                season_id,
            )
            for season_id in row.available_season_ids
        ],
    )


def insert_full_span_rows(
    connection,
    snapshot_id: int,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    connection.executemany(
        """
        INSERT INTO metric_full_span_series (
            snapshot_id,
            player_id,
            player_name,
            span_average_value,
            season_count,
            rank_order
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                snapshot_id,
                row.player_id,
                row.player_name,
                row.span_average_value,
                row.season_count,
                row.rank_order,
            )
            for row in series_rows
        ],
    )
    connection.executemany(
        """
        INSERT INTO metric_full_span_points (
            snapshot_id,
            player_id,
            season_id,
            value
        ) VALUES (?, ?, ?, ?)
        """,
        [
            (
                snapshot_id,
                row.player_id,
                row.season_id,
                row.value,
            )
            for row in point_rows
        ],
    )
