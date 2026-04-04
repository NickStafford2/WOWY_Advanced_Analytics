from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._catalog import MetricScopeCatalogRow
from rawr_analytics.data.metric_store._sql_writes import (
    delete_metric_full_span_rows,
    delete_metric_rows,
    insert_full_span_rows,
    insert_metric_scope_seasons,
    insert_metric_scope_teams,
    insert_metric_snapshot,
)
from rawr_analytics.data.metric_store._validation import (
    validate_metric_full_span_rows,
    validate_metric_scope_catalog_row,
)
from rawr_analytics.data.metric_store.full_span import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
)
from rawr_analytics.data.metric_store.schema import connect, initialize_player_metrics_db


def replace_metric_scope_snapshot(
    *,
    metric_id: str,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    catalog_row: MetricScopeCatalogRow,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
    insert_rows: Callable[[Any, int | None], None],
    row_count: int,
) -> None:
    initialize_player_metrics_db()
    validate_metric_scope_catalog_row(catalog_row)
    validate_metric_full_span_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        series_rows=series_rows,
        point_rows=point_rows,
    )
    updated_at = datetime.now(UTC).isoformat()

    with connect(METRIC_STORE_DB_PATH) as connection:
        connection.execute("BEGIN")
        delete_metric_full_span_rows(connection, metric_id=metric_id, scope_key=scope_key)
        connection.execute(
            "DELETE FROM metric_scope_catalog WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_scope_season WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_scope_team WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        delete_metric_rows(connection, metric_id=metric_id, scope_key=scope_key)
        connection.execute(
            "DELETE FROM metric_snapshot WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        snapshot_id = insert_metric_snapshot(
            connection,
            metric_id=metric_id,
            scope_key=scope_key,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            row_count=row_count,
            updated_at=updated_at,
        )
        insert_rows(connection, snapshot_id)
        connection.execute(
            """
            INSERT INTO metric_scope_catalog (
                metric_id,
                scope_key,
                label,
                team_filter,
                season_type,
                full_span_start_season_id,
                full_span_end_season_id,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                catalog_row.metric_id,
                catalog_row.scope_key,
                catalog_row.label,
                catalog_row.team_filter,
                catalog_row.season_type,
                catalog_row.full_span_start_season_id,
                catalog_row.full_span_end_season_id,
                catalog_row.updated_at,
            ),
        )
        insert_metric_scope_seasons(connection, catalog_row)
        insert_metric_scope_teams(connection, catalog_row)
        insert_full_span_rows(connection, snapshot_id, series_rows, point_rows)
        connection.commit()
