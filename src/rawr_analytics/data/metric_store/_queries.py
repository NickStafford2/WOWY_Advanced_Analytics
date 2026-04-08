from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._catalog import MetricScopeCatalogRow
from rawr_analytics.data.metric_store.full_span import MetricFullSpanSeriesRow
from rawr_analytics.data.metric_store.schema import connect, initialize_player_metrics_db


@dataclass(frozen=True)
class MetricSnapshotState:
    snapshot_id: int | None
    metric_id: str
    scope_key: str
    build_version: str
    source_fingerprint: str
    row_count: int
    updated_at: str


def load_metric_snapshot_state(
    metric: str,
    scope_key: str,
) -> MetricSnapshotState | None:
    initialize_player_metrics_db()
    with connect(METRIC_STORE_DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT
                snapshot_id,
                metric_id,
                scope_key,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            FROM metric_snapshot
            WHERE metric_id = ? AND scope_key = ?
            """,
            (metric, scope_key),
        ).fetchone()
    if row is None:
        return None
    return MetricSnapshotState(
        snapshot_id=row["snapshot_id"],
        metric_id=row["metric_id"],
        scope_key=row["scope_key"],
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
    with connect(METRIC_STORE_DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT
                metric_id,
                scope_key,
                label,
                team_filter,
                season_type,
                full_span_start_season_id,
                full_span_end_season_id,
                updated_at
            FROM metric_scope_catalog
            WHERE metric_id = ? AND scope_key = ?
            """,
            (metric, scope_key),
        ).fetchone()
        team_rows = connection.execute(
            """
            SELECT team_id
            FROM metric_scope_team
            WHERE metric_id = ? AND scope_key = ?
            ORDER BY team_id
            """,
            (metric, scope_key),
        ).fetchall()
        season_rows = connection.execute(
            """
            SELECT season_id
            FROM metric_scope_season
            WHERE metric_id = ? AND scope_key = ?
            ORDER BY season_id
            """,
            (metric, scope_key),
        ).fetchall()
    if row is None:
        return None
    return MetricScopeCatalogRow(
        metric_id=row["metric_id"],
        scope_key=row["scope_key"],
        label=row["label"],
        team_filter=row["team_filter"],
        season_type=row["season_type"],
        available_season_ids=[season_row["season_id"] for season_row in season_rows],
        available_team_ids=[team_row["team_id"] for team_row in team_rows],
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
            series.snapshot_id,
            snapshot.metric_id,
            snapshot.scope_key,
            player_id,
            player_name,
            span_average_value,
            season_count,
            rank_order
        FROM metric_full_span_series AS series
        INNER JOIN metric_snapshot AS snapshot
            ON snapshot.snapshot_id = series.snapshot_id
        WHERE snapshot.metric_id = ? AND snapshot.scope_key = ?
        ORDER BY rank_order
    """
    params: list[Any] = [metric, scope_key]
    if top_n is not None:
        query += " LIMIT ?"
        params.append(top_n)
    with connect(METRIC_STORE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        MetricFullSpanSeriesRow(
            snapshot_id=row["snapshot_id"],
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
            points.player_id,
            points.season_id,
            points.value
        FROM metric_full_span_points AS points
        INNER JOIN metric_snapshot AS snapshot
            ON snapshot.snapshot_id = points.snapshot_id
        WHERE snapshot.metric_id = ?
          AND snapshot.scope_key = ?
          AND points.player_id IN ({placeholders})
    """
    params: list[Any] = [metric, scope_key, *player_ids]
    with connect(METRIC_STORE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    points: dict[int, dict[str, float]] = {}
    for row in rows:
        points.setdefault(row["player_id"], {})[row["season_id"]] = row["value"]
    return points
