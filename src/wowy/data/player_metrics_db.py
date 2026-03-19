from __future__ import annotations

import json
import math
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from wowy.nba.seasons import canonicalize_season_string, season_sort_key
from wowy.nba.season_types import canonicalize_season_type


DEFAULT_PLAYER_METRICS_DB_PATH = Path("data/app/player_metrics.sqlite3")
LEGACY_METRIC_RENAMES = {
    "shrinkage_wowy": ("wowy_shrunk", "WOWY Shrunk"),
}
_TEAM_ABBREVIATION_PATTERN = re.compile(r"^[A-Z]{3}$")


@dataclass(frozen=True)
class PlayerSeasonMetricRow:
    metric: str
    metric_label: str
    scope_key: str
    team_filter: str
    season_type: str
    season: str
    player_id: int
    player_name: str
    value: float
    sample_size: int | None = None
    secondary_sample_size: int | None = None
    average_minutes: float | None = None
    total_minutes: float | None = None
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class MetricStoreMetadata:
    metric: str
    scope_key: str
    metric_label: str
    build_version: str
    source_fingerprint: str
    row_count: int
    updated_at: str


@dataclass(frozen=True)
class MetricScopeCatalogRow:
    metric: str
    scope_key: str
    metric_label: str
    team_filter: str
    season_type: str
    available_seasons: list[str]
    available_teams: list[str]
    full_span_start_season: str | None
    full_span_end_season: str | None
    updated_at: str


@dataclass(frozen=True)
class MetricFullSpanSeriesRow:
    metric: str
    scope_key: str
    player_id: int
    player_name: str
    span_average_value: float
    season_count: int
    rank_order: int


@dataclass(frozen=True)
class MetricFullSpanPointRow:
    metric: str
    scope_key: str
    player_id: int
    season: str
    value: float


def initialize_player_metrics_db(db_path: Path) -> None:
    with _connect(db_path) as connection:
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


def replace_metric_rows(
    db_path: Path,
    *,
    metric: str,
    scope_key: str,
    metric_label: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[PlayerSeasonMetricRow],
) -> None:
    initialize_player_metrics_db(db_path)
    _validate_metric_rows(
        metric=metric,
        scope_key=scope_key,
        metric_label=metric_label,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    updated_at = datetime.now(UTC).isoformat()

    with _connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM metric_player_season_values WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.executemany(
            """
            INSERT INTO metric_player_season_values (
                metric,
                metric_label,
                scope_key,
                team_filter,
                season_type,
                season,
                player_id,
                player_name,
                value,
                sample_size,
                secondary_sample_size,
                average_minutes,
                total_minutes,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric,
                    row.metric_label,
                    row.scope_key,
                    row.team_filter,
                    row.season_type,
                    row.season,
                    row.player_id,
                    row.player_name,
                    row.value,
                    row.sample_size,
                    row.secondary_sample_size,
                    row.average_minutes,
                    row.total_minutes,
                    json.dumps(row.details or {}, sort_keys=True),
                )
                for row in rows
            ],
        )
        connection.execute(
            """
            INSERT INTO metric_store_metadata_v2 (
                metric,
                scope_key,
                metric_label,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric, scope_key) DO UPDATE SET
                metric_label = excluded.metric_label,
                build_version = excluded.build_version,
                source_fingerprint = excluded.source_fingerprint,
                row_count = excluded.row_count,
                updated_at = excluded.updated_at
            """,
            (
                metric,
                scope_key,
                metric_label,
                build_version,
                source_fingerprint,
                len(rows),
                updated_at,
            ),
        )
        connection.commit()


def load_metric_store_metadata(
    db_path: Path,
    metric: str,
    scope_key: str,
) -> MetricStoreMetadata | None:
    initialize_player_metrics_db(db_path)
    with _connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                metric,
                scope_key,
                metric_label,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            FROM metric_store_metadata_v2
            WHERE metric = ? AND scope_key = ?
            """,
            (metric, scope_key),
        ).fetchone()
    if row is None:
        return None
    return MetricStoreMetadata(
        metric=row["metric"],
        scope_key=row["scope_key"],
        metric_label=row["metric_label"],
        build_version=row["build_version"],
        source_fingerprint=row["source_fingerprint"],
        row_count=row["row_count"],
        updated_at=row["updated_at"],
    )


def replace_metric_scope_catalog_row(
    db_path: Path,
    *,
    row: MetricScopeCatalogRow,
) -> None:
    initialize_player_metrics_db(db_path)
    _validate_metric_scope_catalog_row(row)
    with _connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO metric_scope_catalog (
                metric,
                scope_key,
                metric_label,
                team_filter,
                season_type,
                available_seasons_json,
                available_teams_json,
                full_span_start_season,
                full_span_end_season,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric, scope_key) DO UPDATE SET
                metric_label = excluded.metric_label,
                team_filter = excluded.team_filter,
                season_type = excluded.season_type,
                available_seasons_json = excluded.available_seasons_json,
                available_teams_json = excluded.available_teams_json,
                full_span_start_season = excluded.full_span_start_season,
                full_span_end_season = excluded.full_span_end_season,
                updated_at = excluded.updated_at
            """,
            (
                row.metric,
                row.scope_key,
                row.metric_label,
                row.team_filter,
                row.season_type,
                json.dumps(row.available_seasons),
                json.dumps(row.available_teams),
                row.full_span_start_season,
                row.full_span_end_season,
                row.updated_at,
            ),
        )
        connection.commit()


def load_metric_scope_catalog_row(
    db_path: Path,
    metric: str,
    scope_key: str,
) -> MetricScopeCatalogRow | None:
    initialize_player_metrics_db(db_path)
    with _connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                metric,
                scope_key,
                metric_label,
                team_filter,
                season_type,
                available_seasons_json,
                available_teams_json,
                full_span_start_season,
                full_span_end_season,
                updated_at
            FROM metric_scope_catalog
            WHERE metric = ? AND scope_key = ?
            """,
            (metric, scope_key),
        ).fetchone()
    if row is None:
        return None
    return MetricScopeCatalogRow(
        metric=row["metric"],
        scope_key=row["scope_key"],
        metric_label=row["metric_label"],
        team_filter=row["team_filter"],
        season_type=row["season_type"],
        available_seasons=json.loads(row["available_seasons_json"]),
        available_teams=json.loads(row["available_teams_json"]),
        full_span_start_season=row["full_span_start_season"],
        full_span_end_season=row["full_span_end_season"],
        updated_at=row["updated_at"],
    )


def replace_metric_full_span_rows(
    db_path: Path,
    *,
    metric: str,
    scope_key: str,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    initialize_player_metrics_db(db_path)
    _validate_metric_full_span_rows(
        metric=metric,
        scope_key=scope_key,
        series_rows=series_rows,
        point_rows=point_rows,
    )
    with _connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM metric_full_span_points WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_full_span_series WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.executemany(
            """
            INSERT INTO metric_full_span_series (
                metric,
                scope_key,
                player_id,
                player_name,
                span_average_value,
                season_count,
                rank_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric,
                    row.scope_key,
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
                metric,
                scope_key,
                player_id,
                season,
                value
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric,
                    row.scope_key,
                    row.player_id,
                    row.season,
                    row.value,
                )
                for row in point_rows
            ],
        )
        connection.commit()


def load_metric_full_span_series_rows(
    db_path: Path,
    *,
    metric: str,
    scope_key: str,
    top_n: int | None = None,
) -> list[MetricFullSpanSeriesRow]:
    initialize_player_metrics_db(db_path)
    query = """
        SELECT
            metric,
            scope_key,
            player_id,
            player_name,
            span_average_value,
            season_count,
            rank_order
        FROM metric_full_span_series
        WHERE metric = ? AND scope_key = ?
        ORDER BY rank_order
    """
    params: list[Any] = [metric, scope_key]
    if top_n is not None:
        query += " LIMIT ?"
        params.append(top_n)
    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        MetricFullSpanSeriesRow(
            metric=row["metric"],
            scope_key=row["scope_key"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            span_average_value=row["span_average_value"],
            season_count=row["season_count"],
            rank_order=row["rank_order"],
        )
        for row in rows
    ]


def load_metric_full_span_points_map(
    db_path: Path,
    *,
    metric: str,
    scope_key: str,
    player_ids: list[int],
) -> dict[int, dict[str, float]]:
    initialize_player_metrics_db(db_path)
    if not player_ids:
        return {}
    placeholders = ",".join("?" for _ in player_ids)
    query = f"""
        SELECT
            player_id,
            season,
            value
        FROM metric_full_span_points
        WHERE metric = ? AND scope_key = ? AND player_id IN ({placeholders})
    """
    params: list[Any] = [metric, scope_key, *player_ids]
    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    points: dict[int, dict[str, float]] = {}
    for row in rows:
        points.setdefault(row["player_id"], {})[row["season"]] = row["value"]
    return points


def list_metric_seasons(
    db_path: Path,
    metric: str,
    scope_key: str,
) -> list[str]:
    initialize_player_metrics_db(db_path)
    with _connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT season
            FROM metric_player_season_values
            WHERE metric = ? AND scope_key = ?
            ORDER BY season
            """,
            (metric, scope_key),
        ).fetchall()
    return [row["season"] for row in rows]


def load_metric_rows(
    db_path: Path,
    *,
    metric: str,
    scope_key: str,
    seasons: list[str] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_sample_size: int | None = None,
    min_secondary_sample_size: int | None = None,
) -> list[PlayerSeasonMetricRow]:
    initialize_player_metrics_db(db_path)

    query = """
        SELECT
            metric,
            metric_label,
            scope_key,
            team_filter,
            season_type,
            season,
            player_id,
            player_name,
            value,
            sample_size,
            secondary_sample_size,
            average_minutes,
            total_minutes,
            details_json
        FROM metric_player_season_values
        WHERE metric = ? AND scope_key = ?
    """
    params: list[Any] = [metric, scope_key]

    if seasons:
        query += f" AND season IN ({','.join('?' for _ in seasons)})"
        params.extend(seasons)
    if min_average_minutes is not None:
        query += " AND COALESCE(average_minutes, 0.0) >= ?"
        params.append(min_average_minutes)
    if min_total_minutes is not None:
        query += " AND COALESCE(total_minutes, 0.0) >= ?"
        params.append(min_total_minutes)
    if min_sample_size is not None:
        query += " AND COALESCE(sample_size, 0) >= ?"
        params.append(min_sample_size)
    if min_secondary_sample_size is not None:
        query += " AND COALESCE(secondary_sample_size, 0) >= ?"
        params.append(min_secondary_sample_size)

    query += " ORDER BY season, value DESC, player_name ASC"

    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        PlayerSeasonMetricRow(
            metric=row["metric"],
            metric_label=row["metric_label"],
            scope_key=row["scope_key"],
            team_filter=row["team_filter"],
            season_type=row["season_type"],
            season=row["season"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            value=row["value"],
            sample_size=row["sample_size"],
            secondary_sample_size=row["secondary_sample_size"],
            average_minutes=row["average_minutes"],
            total_minutes=row["total_minutes"],
            details=json.loads(row["details_json"]),
        )
        for row in rows
    ]


def _validate_metric_rows(
    *,
    metric: str,
    scope_key: str,
    metric_label: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[PlayerSeasonMetricRow],
) -> None:
    _validate_required_text(metric, "metric")
    _validate_required_text(scope_key, "scope_key")
    _validate_required_text(metric_label, "metric_label")
    _validate_required_text(build_version, "build_version")
    _validate_required_text(source_fingerprint, "source_fingerprint")

    row_keys: set[tuple[str, int]] = set()
    expected_team_filter: str | None = None
    expected_season_type: str | None = None

    for row in rows:
        if row.metric != metric:
            raise ValueError(
                f"Metric row for player {row.player_id!r} has metric {row.metric!r}; "
                f"expected {metric!r}"
            )
        if row.scope_key != scope_key:
            raise ValueError(
                f"Metric row for player {row.player_id!r} has scope_key {row.scope_key!r}; "
                f"expected {scope_key!r}"
            )
        if row.metric_label != metric_label:
            raise ValueError(
                f"Metric row for player {row.player_id!r} has label {row.metric_label!r}; "
                f"expected {metric_label!r}"
            )

        canonical_season_type = canonicalize_season_type(row.season_type)
        if row.season_type != canonical_season_type:
            raise ValueError(
                f"Metric row for player {row.player_id!r} uses non-canonical season_type "
                f"{row.season_type!r}"
            )
        canonical_team_filter = _canonical_team_filter(row.team_filter)
        if row.team_filter != canonical_team_filter:
            raise ValueError(
                f"Metric row for player {row.player_id!r} uses non-canonical team_filter "
                f"{row.team_filter!r}"
            )
        _validate_scope_shape(
            scope_key=row.scope_key,
            team_filter=canonical_team_filter,
            season_type=canonical_season_type,
        )
        canonical_season = canonicalize_season_string(row.season)
        if canonical_season != row.season:
            raise ValueError(
                f"Metric row for player {row.player_id!r} uses non-canonical season "
                f"{row.season!r}"
            )

        if expected_team_filter is None:
            expected_team_filter = canonical_team_filter
        elif canonical_team_filter != expected_team_filter:
            raise ValueError(
                "Metric rows in the same batch must use one canonical team_filter"
            )

        if expected_season_type is None:
            expected_season_type = canonical_season_type
        elif canonical_season_type != expected_season_type:
            raise ValueError(
                "Metric rows in the same batch must use one canonical season_type"
            )

        if row.player_id <= 0:
            raise ValueError(f"Metric row has invalid player_id {row.player_id!r}")
        _validate_required_text(row.player_name, f"player_name for player {row.player_id}")
        if not math.isfinite(row.value):
            raise ValueError(f"Metric row for player {row.player_id!r} has non-finite value")

        _validate_optional_non_negative_int(
            row.sample_size,
            f"sample_size for player {row.player_id}",
        )
        _validate_optional_non_negative_int(
            row.secondary_sample_size,
            f"secondary_sample_size for player {row.player_id}",
        )
        _validate_optional_non_negative_float(
            row.average_minutes,
            f"average_minutes for player {row.player_id}",
        )
        _validate_optional_non_negative_float(
            row.total_minutes,
            f"total_minutes for player {row.player_id}",
        )
        if (
            row.average_minutes is not None
            and row.total_minutes is not None
            and row.total_minutes + 1e-9 < row.average_minutes
        ):
            raise ValueError(
                f"Metric row for player {row.player_id!r} has total_minutes smaller "
                "than average_minutes"
            )
        if row.details is not None and not isinstance(row.details, dict):
            raise ValueError(
                f"Metric row for player {row.player_id!r} must use a dict for details"
            )

        row_key = (row.season, row.player_id)
        if row_key in row_keys:
            raise ValueError(f"Duplicate metric row for {row_key!r}")
        row_keys.add(row_key)

def _validate_metric_scope_catalog_row(row: MetricScopeCatalogRow) -> None:
    _validate_required_text(row.metric, "metric")
    _validate_required_text(row.scope_key, "scope_key")
    _validate_required_text(row.metric_label, "metric_label")
    canonical_season_type = canonicalize_season_type(row.season_type)
    if row.season_type != canonical_season_type:
        raise ValueError("Catalog season_type must use canonical season type")
    canonical_team_filter = _canonical_team_filter(row.team_filter)
    if row.team_filter != canonical_team_filter:
        raise ValueError("Catalog team_filter must use canonical uppercase abbreviations")
    _validate_scope_shape(
        scope_key=row.scope_key,
        team_filter=canonical_team_filter,
        season_type=canonical_season_type,
    )

    seasons = [canonicalize_season_string(season) for season in row.available_seasons]
    if seasons != row.available_seasons:
        raise ValueError("Catalog available_seasons must use canonical season strings")
    if seasons != sorted(set(seasons), key=season_sort_key):
        raise ValueError("Catalog available_seasons must be unique and sorted")

    teams = [_canonical_team(team) for team in row.available_teams]
    if teams != row.available_teams:
        raise ValueError("Catalog available_teams must use canonical uppercase abbreviations")
    if teams != sorted(set(teams)):
        raise ValueError("Catalog available_teams must be unique and sorted")

    if (row.full_span_start_season is None) != (row.full_span_end_season is None):
        raise ValueError("Catalog full-span seasons must both be set or both be null")
    if row.full_span_start_season is not None:
        start = canonicalize_season_string(row.full_span_start_season)
        end = canonicalize_season_string(row.full_span_end_season or "")
        if start not in seasons or end not in seasons:
            raise ValueError("Catalog full-span seasons must be present in available_seasons")
        if season_sort_key(start) > season_sort_key(end):
            raise ValueError("Catalog full-span start season must not be after end season")

    _validate_iso_datetime(row.updated_at, "catalog updated_at")


def _validate_metric_full_span_rows(
    *,
    metric: str,
    scope_key: str,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    _validate_required_text(metric, "metric")
    _validate_required_text(scope_key, "scope_key")
    if not series_rows and point_rows:
        raise ValueError("Full-span points require matching series rows")

    ranks: list[int] = []
    expected_point_counts: dict[int, int] = {}

    for row in series_rows:
        if row.metric != metric or row.scope_key != scope_key:
            raise ValueError("Full-span series rows must match the requested metric scope")
        if row.player_id <= 0:
            raise ValueError(f"Full-span series row has invalid player_id {row.player_id!r}")
        _validate_required_text(
            row.player_name,
            f"full-span player_name for player {row.player_id}",
        )
        if not math.isfinite(row.span_average_value):
            raise ValueError(
                f"Full-span series row for player {row.player_id!r} has non-finite value"
            )
        if row.season_count <= 0:
            raise ValueError(
                f"Full-span series row for player {row.player_id!r} has invalid season_count"
            )
        if row.rank_order <= 0:
            raise ValueError(
                f"Full-span series row for player {row.player_id!r} has invalid rank_order"
            )
        if row.player_id in expected_point_counts:
            raise ValueError(f"Duplicate full-span series row for player {row.player_id!r}")

        expected_point_counts[row.player_id] = row.season_count
        ranks.append(row.rank_order)
    if sorted(ranks) != list(range(1, len(series_rows) + 1)):
        raise ValueError("Full-span series rank_order values must be unique and contiguous")

    points_by_player: dict[int, set[str]] = defaultdict(set)
    for row in point_rows:
        if row.metric != metric or row.scope_key != scope_key:
            raise ValueError("Full-span point rows must match the requested metric scope")
        if row.player_id not in expected_point_counts:
            raise ValueError(
                f"Full-span point row for unknown player {row.player_id!r}"
            )
        canonical_season = canonicalize_season_string(row.season)
        if canonical_season != row.season:
            raise ValueError(
                f"Full-span point row for player {row.player_id!r} uses non-canonical "
                f"season {row.season!r}"
            )
        if not math.isfinite(row.value):
            raise ValueError(
                f"Full-span point row for player {row.player_id!r} has non-finite value"
            )
        if row.season in points_by_player[row.player_id]:
            raise ValueError(
                f"Duplicate full-span point row for player {row.player_id!r} and "
                f"season {row.season!r}"
            )
        points_by_player[row.player_id].add(row.season)

    for player_id, season_count in expected_point_counts.items():
        if len(points_by_player[player_id]) != season_count:
            raise ValueError(
                f"Full-span player {player_id!r} expected {season_count} season points "
                f"but found {len(points_by_player[player_id])}"
            )


def _connect(db_path: Path) -> sqlite3.Connection:
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


def _validate_scope_shape(*, scope_key: str, team_filter: str, season_type: str) -> None:
    expected_team_key = team_filter or "all-teams"
    expected_scope_key = f"teams={expected_team_key}|season_type={season_type}"
    if scope_key != expected_scope_key:
        raise ValueError(
            f"Invalid scope_key {scope_key!r}; expected canonical {expected_scope_key!r}"
        )
    if not team_filter:
        return
    teams = team_filter.split(",")
    if teams != sorted(set(teams)):
        raise ValueError("team_filter must be unique and sorted")
    for team in teams:
        _canonical_team(team)


def _canonical_team_filter(value: str) -> str:
    if not value:
        return ""
    teams = value.split(",")
    canonical_teams = [_canonical_team(team) for team in teams]
    if canonical_teams != sorted(set(canonical_teams)):
        raise ValueError("team_filter must be unique and sorted")
    return ",".join(canonical_teams)


def _canonical_team(value: str) -> str:
    team = value.strip().upper()
    if not _TEAM_ABBREVIATION_PATTERN.fullmatch(team):
        raise ValueError(f"Invalid team abbreviation {value!r}")
    return team


def _validate_required_text(value: str, label: str) -> None:
    if not value.strip():
        raise ValueError(f"{label} must not be empty")


def _validate_optional_non_negative_int(value: int | None, label: str) -> None:
    if value is None:
        return
    if value < 0:
        raise ValueError(f"{label} must not be negative")


def _validate_optional_non_negative_float(value: float | None, label: str) -> None:
    if value is None:
        return
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"{label} must be a finite non-negative number")


def _validate_iso_datetime(value: str, label: str) -> None:
    _validate_required_text(value, label)
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO datetime") from exc
