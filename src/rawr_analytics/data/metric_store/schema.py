from __future__ import annotations

import sqlite3
from pathlib import Path

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH


def initialize_player_metrics_db() -> None:
    with connect(METRIC_STORE_DB_PATH) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metric_snapshot (
                snapshot_id INTEGER PRIMARY KEY,
                metric_id TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                build_version TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (metric_id, scope_key)
            );

            CREATE TABLE IF NOT EXISTS rawr_player_season_values (
                snapshot_id INTEGER NOT NULL,
                team_filter TEXT NOT NULL DEFAULT '',
                season_type TEXT NOT NULL DEFAULT 'Regular Season',
                season_id TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                coefficient REAL NOT NULL,
                games INTEGER NOT NULL,
                average_minutes REAL,
                total_minutes REAL,
                PRIMARY KEY (snapshot_id, season_id, player_id)
            );

            CREATE INDEX IF NOT EXISTS idx_rawr_player_season_values_snapshot
            ON rawr_player_season_values (snapshot_id, season_id, player_id);

            CREATE TABLE IF NOT EXISTS wowy_player_season_values (
                snapshot_id INTEGER NOT NULL,
                team_filter TEXT NOT NULL DEFAULT '',
                season_type TEXT NOT NULL DEFAULT 'Regular Season',
                season_id TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                value REAL NOT NULL,
                games_with INTEGER NOT NULL,
                games_without INTEGER NOT NULL,
                avg_margin_with REAL NOT NULL,
                avg_margin_without REAL NOT NULL,
                average_minutes REAL,
                total_minutes REAL,
                raw_wowy_score REAL,
                PRIMARY KEY (snapshot_id, season_id, player_id)
            );

            CREATE INDEX IF NOT EXISTS idx_wowy_player_season_values_snapshot
            ON wowy_player_season_values (snapshot_id, season_id, player_id);

            CREATE TABLE IF NOT EXISTS metric_scope_catalog (
                metric_id TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                label TEXT NOT NULL,
                team_filter TEXT NOT NULL DEFAULT '',
                season_type TEXT NOT NULL DEFAULT 'Regular Season',
                full_span_start_season_id TEXT,
                full_span_end_season_id TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (metric_id, scope_key)
            );

            CREATE TABLE IF NOT EXISTS metric_scope_team (
                metric_id TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                team_id INTEGER NOT NULL,
                PRIMARY KEY (metric_id, scope_key, team_id)
            );

            CREATE INDEX IF NOT EXISTS idx_metric_scope_team_metric_scope
            ON metric_scope_team (metric_id, scope_key);

            CREATE TABLE IF NOT EXISTS metric_scope_season (
                metric_id TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                season_id TEXT NOT NULL,
                PRIMARY KEY (metric_id, scope_key, season_id)
            );

            CREATE INDEX IF NOT EXISTS idx_metric_scope_season_metric_scope
            ON metric_scope_season (metric_id, scope_key);

            CREATE TABLE IF NOT EXISTS metric_full_span_series (
                snapshot_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                span_average_value REAL NOT NULL,
                season_count INTEGER NOT NULL,
                rank_order INTEGER NOT NULL,
                PRIMARY KEY (snapshot_id, player_id)
            );

            CREATE INDEX IF NOT EXISTS idx_metric_full_span_series_snapshot_rank
            ON metric_full_span_series (snapshot_id, rank_order);

            CREATE TABLE IF NOT EXISTS metric_full_span_points (
                snapshot_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                season_id TEXT NOT NULL,
                value REAL NOT NULL,
                PRIMARY KEY (snapshot_id, player_id, season_id)
            );

            CREATE INDEX IF NOT EXISTS idx_metric_full_span_points_snapshot_player
            ON metric_full_span_points (snapshot_id, player_id);
            """
        )


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection
