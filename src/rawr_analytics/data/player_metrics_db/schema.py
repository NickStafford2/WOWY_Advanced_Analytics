from __future__ import annotations

import sqlite3
from pathlib import Path

from rawr_analytics.data.constants import DB_PATH
from rawr_analytics.data.player_metrics_db.constants import LEGACY_METRIC_RENAMES


def initialize_player_metrics_db() -> None:
    with connect(DB_PATH) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metric_player_season_values (
                metric TEXT NOT NULL,
                metric_label TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                team_filter TEXT NOT NULL DEFAULT '',
                season_type TEXT NOT NULL DEFAULT 'Regular Season',
                season TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                value REAL NOT NULL,
                sample_size INTEGER,
                secondary_sample_size INTEGER,
                average_minutes REAL,
                total_minutes REAL,
                details_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (metric, scope_key, season, player_id)
            );

            CREATE INDEX IF NOT EXISTS idx_metric_player_season_values_metric_scope_season
            ON metric_player_season_values (metric, scope_key, season);

            CREATE INDEX IF NOT EXISTS idx_metric_player_season_values_metric_scope_player
            ON metric_player_season_values (metric, scope_key, player_id, season);

            CREATE TABLE IF NOT EXISTS metric_store_metadata_v2 (
                metric TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                metric_label TEXT NOT NULL,
                build_version TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (metric, scope_key)
            );

            CREATE TABLE IF NOT EXISTS metric_scope_catalog (
                metric TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                metric_label TEXT NOT NULL,
                team_filter TEXT NOT NULL DEFAULT '',
                season_type TEXT NOT NULL DEFAULT 'Regular Season',
                available_seasons_json TEXT NOT NULL DEFAULT '[]',
                available_teams_json TEXT NOT NULL DEFAULT '[]',
                full_span_start_season TEXT,
                full_span_end_season TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (metric, scope_key)
            );

            CREATE TABLE IF NOT EXISTS metric_full_span_series (
                metric TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                span_average_value REAL NOT NULL,
                season_count INTEGER NOT NULL,
                rank_order INTEGER NOT NULL,
                PRIMARY KEY (metric, scope_key, player_id)
            );

            CREATE INDEX IF NOT EXISTS idx_metric_full_span_series_metric_scope_rank
            ON metric_full_span_series (metric, scope_key, rank_order);

            CREATE TABLE IF NOT EXISTS metric_full_span_points (
                metric TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                season TEXT NOT NULL,
                value REAL NOT NULL,
                PRIMARY KEY (metric, scope_key, player_id, season)
            );

            CREATE INDEX IF NOT EXISTS idx_metric_full_span_points_metric_scope_player
            ON metric_full_span_points (metric, scope_key, player_id);
            """
        )
        _migrate_legacy_metric_names(connection)


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _migrate_legacy_metric_names(connection: sqlite3.Connection) -> None:
    for old_metric, (new_metric, new_label) in LEGACY_METRIC_RENAMES.items():
        if old_metric == new_metric:
            continue
        connection.execute("BEGIN")
        connection.execute(
            """
            UPDATE OR REPLACE metric_player_season_values
            SET metric = ?, metric_label = ?
            WHERE metric = ?
            """,
            (new_metric, new_label, old_metric),
        )
        connection.execute(
            """
            UPDATE OR REPLACE metric_store_metadata_v2
            SET metric = ?, metric_label = ?
            WHERE metric = ?
            """,
            (new_metric, new_label, old_metric),
        )
        connection.execute(
            """
            UPDATE OR REPLACE metric_scope_catalog
            SET metric = ?, metric_label = ?
            WHERE metric = ?
            """,
            (new_metric, new_label, old_metric),
        )
        connection.execute(
            """
            UPDATE OR REPLACE metric_full_span_series
            SET metric = ?
            WHERE metric = ?
            """,
            (new_metric, old_metric),
        )
        connection.execute(
            """
            UPDATE OR REPLACE metric_full_span_points
            SET metric = ?
            WHERE metric = ?
            """,
            (new_metric, old_metric),
        )
        connection.commit()
