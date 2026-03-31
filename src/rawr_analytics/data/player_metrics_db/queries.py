from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from rawr_analytics.data.constants import DB_PATH
from rawr_analytics.data.player_metrics_db.models import (
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    MetricStoreMetadata,
    PlayerSeasonMetricRow,
)
from rawr_analytics.data.player_metrics_db.schema import connect, initialize_player_metrics_db


def load_metric_store_metadata(
    metric: str,
    scope_key: str,
) -> MetricStoreMetadata | None:
    initialize_player_metrics_db()
    with connect(DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT
                metric_id,
                scope_key,
                label,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            FROM metric_store_metadata_v2
            WHERE metric_id = ? AND scope_key = ?
            """,
            (metric, scope_key),
        ).fetchone()
    if row is None:
        return None
    return MetricStoreMetadata(
        metric_id=row["metric_id"],
        scope_key=row["scope_key"],
        label=row["label"],
        build_version=row["build_version"],
        source_fingerprint=row["source_fingerprint"],
        row_count=row["row_count"],
        updated_at=row["updated_at"],
    )


def load_metric_scope_catalog_row(
    metric: str,
    scope_key: str,
) -> MetricScopeCatalogRow | None:
    initialize_player_metrics_db()
    with connect(DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT
                metric_id,
                scope_key,
                label,
                team_filter,
                season_type,
                available_season_ids_json,
                available_team_ids_json,
                full_span_start_season_id,
                full_span_end_season_id,
                updated_at
            FROM metric_scope_catalog
            WHERE metric_id = ? AND scope_key = ?
            """,
            (metric, scope_key),
        ).fetchone()
    if row is None:
        return None
    return MetricScopeCatalogRow(
        metric_id=row["metric_id"],
        scope_key=row["scope_key"],
        label=row["label"],
        team_filter=row["team_filter"],
        season_type=row["season_type"],
        available_season_ids=json.loads(row["available_season_ids_json"]),
        available_team_ids=json.loads(row["available_team_ids_json"]),
        full_span_start_season_id=row["full_span_start_season_id"],
        full_span_end_season_id=row["full_span_end_season_id"],
        updated_at=row["updated_at"],
    )


def load_metric_full_span_series_rows(
    *,
    metric: str,
    scope_key: str,
    top_n: int | None = None,
) -> list[MetricFullSpanSeriesRow]:
    initialize_player_metrics_db()
    query = """
        SELECT
            metric_id,
            scope_key,
            player_id,
            player_name,
            span_average_value,
            season_count,
            rank_order
        FROM metric_full_span_series
        WHERE metric_id = ? AND scope_key = ?
        ORDER BY rank_order
    """
    params: list[Any] = [metric, scope_key]
    if top_n is not None:
        query += " LIMIT ?"
        params.append(top_n)
    with connect(DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        MetricFullSpanSeriesRow(
            metric_id=row["metric_id"],
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
    *,
    metric: str,
    scope_key: str,
    player_ids: list[int],
) -> dict[int, dict[str, float]]:
    initialize_player_metrics_db()
    if not player_ids:
        return {}
    placeholders = ",".join("?" for _ in player_ids)
    query = f"""
        SELECT
            player_id,
            season_id,
            value
        FROM metric_full_span_points
        WHERE metric_id = ? AND scope_key = ? AND player_id IN ({placeholders})
    """
    params: list[Any] = [metric, scope_key, *player_ids]
    with connect(DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    points: dict[int, dict[str, float]] = {}
    for row in rows:
        points.setdefault(row["player_id"], {})[row["season_id"]] = row["value"]
    return points


def _list_metric_seasons(
    db_path: Path,
    metric: str,
    scope_key: str,
) -> list[str]:
    initialize_player_metrics_db()
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT season_id
            FROM metric_player_season_values
            WHERE metric_id = ? AND scope_key = ?
            ORDER BY season_id
            """,
            (metric, scope_key),
        ).fetchall()
    return [row["season_id"] for row in rows]


def load_metric_rows(
    *,
    metric: str,
    scope_key: str,
    seasons: list[str] | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    min_sample_size: int | None = None,
    min_secondary_sample_size: int | None = None,
) -> list[PlayerSeasonMetricRow]:
    initialize_player_metrics_db()

    query = """
        SELECT
            metric_id,
            scope_key,
            team_filter,
            season_type,
            season_id,
            player_id,
            player_name,
            value,
            sample_size,
            secondary_sample_size,
            average_minutes,
            total_minutes,
            details_json
        FROM metric_player_season_values
        WHERE metric_id = ? AND scope_key = ?
    """
    params: list[Any] = [metric, scope_key]

    if seasons:
        query += f" AND season_id IN ({','.join('?' for _ in seasons)})"
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

    query += " ORDER BY season_id, value DESC, player_name ASC"

    with connect(DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        PlayerSeasonMetricRow(
            metric_id=row["metric_id"],
            scope_key=row["scope_key"],
            team_filter=row["team_filter"],
            season_type=row["season_type"],
            season_id=row["season_id"],
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
