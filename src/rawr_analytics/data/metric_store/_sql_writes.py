from __future__ import annotations

import sqlite3

from rawr_analytics.data.metric_store._catalog import MetricCacheCatalogRow
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
    metric_values_table,
)

def delete_metric_value_rows(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    metric_cache_key: str,
) -> None:
    table = metric_values_table(metric_id)
    connection.execute(
        f"""
        DELETE FROM {table}
        WHERE metric_cache_entry_id IN (
            SELECT metric_cache_entry_id
            FROM metric_cache_entry
            WHERE metric_id = ? AND metric_cache_key = ?
        )
        """,
        (metric_id, metric_cache_key),
    )


def delete_metric_rows(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    metric_cache_key: str,
) -> None:
    delete_metric_value_rows(
        connection,
        metric_id=metric_id,
        metric_cache_key=metric_cache_key,
    )


def delete_metric_cache_rows(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    metric_cache_key: str,
) -> None:
    connection.execute(
        "DELETE FROM metric_cache_catalog WHERE metric_id = ? AND metric_cache_key = ?",
        (metric_id, metric_cache_key),
    )
    connection.execute(
        "DELETE FROM metric_cache_season WHERE metric_id = ? AND metric_cache_key = ?",
        (metric_id, metric_cache_key),
    )
    connection.execute(
        "DELETE FROM metric_cache_team WHERE metric_id = ? AND metric_cache_key = ?",
        (metric_id, metric_cache_key),
    )
    delete_metric_rows(connection, metric_id=metric_id, metric_cache_key=metric_cache_key)
    connection.execute(
        "DELETE FROM metric_cache_entry WHERE metric_id = ? AND metric_cache_key = ?",
        (metric_id, metric_cache_key),
    )


def insert_metric_cache_entry(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    metric_cache_key: str,
    build_version: str,
    source_fingerprint: str,
    row_count: int,
    updated_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO metric_cache_entry (
            metric_id,
            metric_cache_key,
            build_version,
            source_fingerprint,
            row_count,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            metric_id,
            metric_cache_key,
            build_version,
            source_fingerprint,
            row_count,
            updated_at,
        ),
    )
    assert cursor.lastrowid is not None, "metric_cache_entry insert must produce row id"
    return int(cursor.lastrowid)


def insert_rawr_rows(
    connection,
    rows: list[RawrPlayerSeasonValueRow],
    metric_cache_entry_id: int | None,
) -> None:
    if not rows:
        return
    assert metric_cache_entry_id is not None, "rawr cache writes require metric_cache_entry_id"
    connection.executemany(
        """
        INSERT INTO rawr_player_season_values (
            metric_cache_entry_id,
            season_id,
            player_id,
            player_name,
            coefficient,
            games,
            average_minutes,
            total_minutes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                metric_cache_entry_id,
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
    metric_cache_entry_id: int | None,
) -> None:
    if not rows:
        return
    assert metric_cache_entry_id is not None, "wowy cache writes require metric_cache_entry_id"
    connection.executemany(
        """
        INSERT INTO wowy_player_season_values (
            metric_cache_entry_id,
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                metric_cache_entry_id,
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


def insert_metric_cache_teams(connection, row: MetricCacheCatalogRow) -> None:
    if not row.available_team_ids:
        return
    connection.executemany(
        """
        INSERT INTO metric_cache_team (
            metric_id,
            metric_cache_key,
            team_id
        ) VALUES (?, ?, ?)
        """,
        [
            (
                row.metric_id,
                row.metric_cache_key,
                team_id,
            )
            for team_id in row.available_team_ids
        ],
    )


def insert_metric_cache_seasons(connection, row: MetricCacheCatalogRow) -> None:
    if not row.available_season_ids:
        return
    connection.executemany(
        """
        INSERT INTO metric_cache_season (
            metric_id,
            metric_cache_key,
            season_id
        ) VALUES (?, ?, ?)
        """,
        [
            (
                row.metric_id,
                row.metric_cache_key,
                season_id,
            )
            for season_id in row.available_season_ids
        ],
    )
