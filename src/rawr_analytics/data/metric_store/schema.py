from __future__ import annotations

import sqlite3
from pathlib import Path

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH


def initialize_metric_store_db() -> None:
    with connect(METRIC_STORE_DB_PATH) as connection:
        connection.executescript(
            """
            DROP INDEX IF EXISTS idx_metric_full_span_series_snapshot_rank;
            DROP INDEX IF EXISTS idx_metric_full_span_points_snapshot_player;
            DROP TABLE IF EXISTS metric_full_span_series;
            DROP TABLE IF EXISTS metric_full_span_points;
            DROP TABLE IF EXISTS metric_cache_catalog;
            DROP TABLE IF EXISTS metric_cache_season;

            CREATE TABLE IF NOT EXISTS metric_cache_entry (
                metric_cache_entry_id INTEGER PRIMARY KEY,
                metric_id TEXT NOT NULL,
                metric_cache_key TEXT NOT NULL,
                build_version TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (metric_id, metric_cache_key)
            );

            CREATE TABLE IF NOT EXISTS rawr_player_season_values (
                metric_cache_entry_id INTEGER NOT NULL,
                season_id TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                coefficient REAL NOT NULL,
                games INTEGER NOT NULL,
                average_minutes REAL,
                total_minutes REAL,
                PRIMARY KEY (metric_cache_entry_id, season_id, player_id)
            );

            CREATE INDEX IF NOT EXISTS idx_rawr_player_season_values_cache_entry
            ON rawr_player_season_values (metric_cache_entry_id, season_id, player_id);

            CREATE TABLE IF NOT EXISTS wowy_player_season_values (
                metric_cache_entry_id INTEGER NOT NULL,
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
                PRIMARY KEY (metric_cache_entry_id, season_id, player_id)
            );

            CREATE INDEX IF NOT EXISTS idx_wowy_player_season_values_cache_entry
            ON wowy_player_season_values (metric_cache_entry_id, season_id, player_id);

            """
        )


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection
