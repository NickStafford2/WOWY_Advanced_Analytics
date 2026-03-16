from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_PLAYER_METRICS_DB_PATH = Path("data/app/player_metrics.sqlite3")


@dataclass(frozen=True)
class PlayerSeasonMetricRow:
    metric: str
    metric_label: str
    season: str
    player_id: int
    player_name: str
    value: float
    games_with: int | None = None
    games_without: int | None = None
    average_minutes: float | None = None
    total_minutes: float | None = None
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class MetricStoreMetadata:
    metric: str
    metric_label: str
    source_fingerprint: str
    row_count: int
    updated_at: str


def initialize_player_metrics_db(db_path: Path) -> None:
    with _connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS player_season_metrics (
                metric TEXT NOT NULL,
                metric_label TEXT NOT NULL,
                season TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                value REAL NOT NULL,
                games_with INTEGER,
                games_without INTEGER,
                average_minutes REAL,
                total_minutes REAL,
                details_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (metric, season, player_id)
            );

            CREATE INDEX IF NOT EXISTS idx_player_season_metrics_metric_season
            ON player_season_metrics (metric, season);

            CREATE INDEX IF NOT EXISTS idx_player_season_metrics_metric_player
            ON player_season_metrics (metric, player_id, season);

            CREATE TABLE IF NOT EXISTS metric_store_metadata (
                metric TEXT PRIMARY KEY,
                metric_label TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )


def replace_metric_rows(
    db_path: Path,
    *,
    metric: str,
    metric_label: str,
    source_fingerprint: str,
    rows: list[PlayerSeasonMetricRow],
) -> None:
    initialize_player_metrics_db(db_path)
    updated_at = datetime.now(UTC).isoformat()

    with _connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM player_season_metrics WHERE metric = ?",
            (metric,),
        )
        connection.executemany(
            """
            INSERT INTO player_season_metrics (
                metric,
                metric_label,
                season,
                player_id,
                player_name,
                value,
                games_with,
                games_without,
                average_minutes,
                total_minutes,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric,
                    row.metric_label,
                    row.season,
                    row.player_id,
                    row.player_name,
                    row.value,
                    row.games_with,
                    row.games_without,
                    row.average_minutes,
                    row.total_minutes,
                    json.dumps(row.details or {}, sort_keys=True),
                )
                for row in rows
            ],
        )
        connection.execute(
            """
            INSERT INTO metric_store_metadata (
                metric,
                metric_label,
                source_fingerprint,
                row_count,
                updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(metric) DO UPDATE SET
                metric_label = excluded.metric_label,
                source_fingerprint = excluded.source_fingerprint,
                row_count = excluded.row_count,
                updated_at = excluded.updated_at
            """,
            (metric, metric_label, source_fingerprint, len(rows), updated_at),
        )
        connection.commit()


def load_metric_store_metadata(
    db_path: Path,
    metric: str,
) -> MetricStoreMetadata | None:
    initialize_player_metrics_db(db_path)
    with _connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT metric, metric_label, source_fingerprint, row_count, updated_at
            FROM metric_store_metadata
            WHERE metric = ?
            """,
            (metric,),
        ).fetchone()
    if row is None:
        return None
    return MetricStoreMetadata(
        metric=row["metric"],
        metric_label=row["metric_label"],
        source_fingerprint=row["source_fingerprint"],
        row_count=row["row_count"],
        updated_at=row["updated_at"],
    )


def list_metric_seasons(
    db_path: Path,
    metric: str,
) -> list[str]:
    initialize_player_metrics_db(db_path)
    with _connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT season
            FROM player_season_metrics
            WHERE metric = ?
            ORDER BY season
            """,
            (metric,),
        ).fetchall()
    return [row["season"] for row in rows]


def load_metric_rows(
    db_path: Path,
    *,
    metric: str,
    seasons: list[str] | None = None,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[PlayerSeasonMetricRow]:
    initialize_player_metrics_db(db_path)

    query = """
        SELECT
            metric,
            metric_label,
            season,
            player_id,
            player_name,
            value,
            games_with,
            games_without,
            average_minutes,
            total_minutes,
            details_json
        FROM player_season_metrics
        WHERE metric = ?
    """
    params: list[Any] = [metric]

    if seasons:
        query += f" AND season IN ({','.join('?' for _ in seasons)})"
        params.extend(seasons)
    if min_games_with is not None:
        query += " AND COALESCE(games_with, 0) >= ?"
        params.append(min_games_with)
    if min_games_without is not None:
        query += " AND COALESCE(games_without, 0) >= ?"
        params.append(min_games_without)
    if min_average_minutes is not None:
        query += " AND COALESCE(average_minutes, 0.0) >= ?"
        params.append(min_average_minutes)
    if min_total_minutes is not None:
        query += " AND COALESCE(total_minutes, 0.0) >= ?"
        params.append(min_total_minutes)

    query += " ORDER BY season, value DESC, player_name ASC"

    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        PlayerSeasonMetricRow(
            metric=row["metric"],
            metric_label=row["metric_label"],
            season=row["season"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            value=row["value"],
            games_with=row["games_with"],
            games_without=row["games_without"],
            average_minutes=row["average_minutes"],
            total_minutes=row["total_minutes"],
            details=json.loads(row["details_json"]),
        )
        for row in rows
    ]


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection
