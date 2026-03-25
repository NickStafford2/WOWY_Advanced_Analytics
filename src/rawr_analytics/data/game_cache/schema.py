from __future__ import annotations

import sqlite3
from pathlib import Path

from rawr_analytics.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH


def initialize_game_cache_db(db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH) -> None:
    with _connect(db_path) as connection:
        _migrate_cache_schema_if_needed(connection)
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS team_history (
                team_id INTEGER NOT NULL,
                season TEXT NOT NULL,
                abbreviation TEXT NOT NULL,
                franchise_id TEXT NOT NULL,
                lookup_abbreviation TEXT NOT NULL,
                PRIMARY KEY (team_id, season),
                UNIQUE (season, abbreviation)
            );

            CREATE INDEX IF NOT EXISTS idx_team_history_season_lookup
            ON team_history (season, lookup_abbreviation);

            CREATE TABLE IF NOT EXISTS normalized_games (
                game_id TEXT NOT NULL,
                season TEXT NOT NULL,
                game_date TEXT NOT NULL,
                team_id INTEGER NOT NULL,
                opponent_team_id INTEGER NOT NULL,
                is_home INTEGER NOT NULL,
                margin REAL NOT NULL,
                season_type TEXT NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (game_id, team_id, season, season_type)
            );

            CREATE INDEX IF NOT EXISTS idx_normalized_games_game_id
            ON normalized_games (game_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_games_season_type_season_team
            ON normalized_games (season_type, season, team_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_games_team_opponent_lookup
            ON normalized_games (season_type, season, team_id, opponent_team_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_games_opponent_lookup
            ON normalized_games (season_type, season, opponent_team_id, team_id);

            CREATE TABLE IF NOT EXISTS normalized_game_players (
                game_id TEXT NOT NULL,
                season TEXT NOT NULL,
                season_type TEXT NOT NULL,
                team_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                appeared INTEGER NOT NULL,
                minutes REAL,
                PRIMARY KEY (game_id, team_id, player_id, season, season_type)
            );

            CREATE INDEX IF NOT EXISTS idx_normalized_game_players_game_id
            ON normalized_game_players (game_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_game_players_season_player
            ON normalized_game_players (season, player_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_game_players_season_type_season_team
            ON normalized_game_players (season_type, season, team_id);

            CREATE TABLE IF NOT EXISTS normalized_cache_loads (
                team_id INTEGER NOT NULL,
                season TEXT NOT NULL,
                season_type TEXT NOT NULL,
                source_path TEXT NOT NULL,
                source_snapshot TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                build_version TEXT NOT NULL,
                refreshed_at TEXT NOT NULL,
                games_row_count INTEGER NOT NULL,
                game_players_row_count INTEGER NOT NULL,
                expected_games_row_count INTEGER,
                skipped_games_row_count INTEGER,
                PRIMARY KEY (team_id, season, season_type)
            );

            CREATE INDEX IF NOT EXISTS idx_normalized_cache_loads_source_path
            ON normalized_cache_loads (source_path);
            """
        )


def _migrate_cache_schema_if_needed(connection: sqlite3.Connection) -> None:
    normalized_games_pk = _primary_key_columns(connection, "normalized_games")
    normalized_players_pk = _primary_key_columns(connection, "normalized_game_players")
    normalized_cache_loads_pk = _primary_key_columns(connection, "normalized_cache_loads")
    team_history_pk = _primary_key_columns(connection, "team_history")
    expected_pks = (
        (
            normalized_games_pk,
            ["game_id", "team_id", "season", "season_type"],
        ),
        (
            normalized_players_pk,
            ["game_id", "team_id", "player_id", "season", "season_type"],
        ),
        (
            normalized_cache_loads_pk,
            ["team_id", "season", "season_type"],
        ),
        (
            team_history_pk,
            ["team_id", "season"],
        ),
    )
    if any(actual and actual != expected for actual, expected in expected_pks):
        _drop_cache_tables(connection)
        return

    required_columns = (
        ("team_history", "team_id"),
        ("team_history", "season"),
        ("team_history", "abbreviation"),
        ("team_history", "franchise_id"),
        ("team_history", "lookup_abbreviation"),
        ("normalized_games", "team_id"),
        ("normalized_games", "opponent_team_id"),
        ("normalized_game_players", "team_id"),
        ("normalized_cache_loads", "team_id"),
    )
    if any(
        _table_exists(connection, table_name)
        and not _table_has_column(connection, table_name, column_name)
        for table_name, column_name in required_columns
    ):
        _drop_cache_tables(connection)
        return

    deprecated_columns = (
        ("normalized_games", "team"),
        ("normalized_games", "opponent"),
        ("normalized_game_players", "team"),
        ("normalized_cache_loads", "team"),
    )
    if any(
        _table_exists(connection, table_name)
        and _table_has_column(connection, table_name, column_name)
        for table_name, column_name in deprecated_columns
    ):
        _drop_cache_tables(connection)
        return

    if _table_exists(connection, "normalized_cache_loads") and not _table_has_column(
        connection,
        "normalized_cache_loads",
        "expected_games_row_count",
    ):
        connection.execute(
            """
            ALTER TABLE normalized_cache_loads
            ADD COLUMN expected_games_row_count INTEGER
            """
        )
    if _table_exists(connection, "normalized_cache_loads") and not _table_has_column(
        connection,
        "normalized_cache_loads",
        "skipped_games_row_count",
    ):
        connection.execute(
            """
            ALTER TABLE normalized_cache_loads
            ADD COLUMN skipped_games_row_count INTEGER
            """
        )


def _drop_cache_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        DROP TABLE IF EXISTS normalized_cache_loads;
        DROP TABLE IF EXISTS normalized_game_players;
        DROP TABLE IF EXISTS normalized_games;
        DROP TABLE IF EXISTS team_history;
        """
    )


def _primary_key_columns(connection: sqlite3.Connection, table_name: str) -> list[str]:
    table_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    if table_exists is None:
        return []
    columns = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [
        column["name"]
        for column in sorted(columns, key=lambda item: item["pk"])
        if column["pk"] > 0
    ]


def _table_has_column(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
) -> bool:
    if not _table_exists(connection, table_name):
        return False
    columns = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(column["name"] == column_name for column in columns)


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    table_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return table_exists is not None


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


__all__ = ["initialize_game_cache_db"]
