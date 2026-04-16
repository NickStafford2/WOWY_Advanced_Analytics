from __future__ import annotations

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._catalog import MetricScopeCatalogRow
from rawr_analytics.data.metric_store._sql_writes import (
    delete_metric_scope_snapshot,
    insert_full_span_rows,
    insert_metric_scope_seasons,
    insert_metric_scope_teams,
    insert_metric_snapshot,
    insert_rawr_rows,
    insert_wowy_rows,
)
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
)
from rawr_analytics.data.metric_store._validation import (
    validate_metric_full_span_rows,
    validate_metric_scope_catalog_row,
)
from rawr_analytics.data.metric_store.full_span import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
)
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db


def replace_rawr_scope_snapshot(
    *,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    updated_at: str,
    catalog_row: MetricScopeCatalogRow,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
    rows: list[RawrPlayerSeasonValueRow],
    row_count: int,
) -> None:
    _validate_metric_scope_snapshot(
        metric_id="rawr",
        scope_key=scope_key,
        updated_at=updated_at,
        catalog_row=catalog_row,
        series_rows=series_rows,
        point_rows=point_rows,
    )
    with connect(METRIC_STORE_DB_PATH) as connection:
        snapshot_id = _begin_metric_scope_replace(
            connection=connection,
            metric_id="rawr",
            scope_key=scope_key,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            updated_at=updated_at,
            row_count=row_count,
        )
        insert_rawr_rows(connection, rows, snapshot_id)
        _finish_metric_scope_replace(
            connection=connection,
            snapshot_id=snapshot_id,
            catalog_row=catalog_row,
            series_rows=series_rows,
            point_rows=point_rows,
        )


def replace_wowy_scope_snapshot(
    *,
    metric_id: str,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    updated_at: str,
    catalog_row: MetricScopeCatalogRow,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
    rows: list[WowyPlayerSeasonValueRow],
    row_count: int,
) -> None:
    _validate_metric_scope_snapshot(
        metric_id=metric_id,
        scope_key=scope_key,
        updated_at=updated_at,
        catalog_row=catalog_row,
        series_rows=series_rows,
        point_rows=point_rows,
    )
    with connect(METRIC_STORE_DB_PATH) as connection:
        snapshot_id = _begin_metric_scope_replace(
            connection=connection,
            metric_id=metric_id,
            scope_key=scope_key,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            updated_at=updated_at,
            row_count=row_count,
        )
        insert_wowy_rows(connection, rows, snapshot_id)
        _finish_metric_scope_replace(
            connection=connection,
            snapshot_id=snapshot_id,
            catalog_row=catalog_row,
            series_rows=series_rows,
            point_rows=point_rows,
        )


def _validate_metric_scope_snapshot(
    *,
    metric_id: str,
    scope_key: str,
    updated_at: str,
    catalog_row: MetricScopeCatalogRow,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    initialize_metric_store_db()
    validate_metric_scope_catalog_row(catalog_row)
    validate_metric_full_span_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        series_rows=series_rows,
        point_rows=point_rows,
    )
    if catalog_row.updated_at != updated_at:
        raise ValueError("Metric scope snapshot requires one shared updated_at timestamp")


def _begin_metric_scope_replace(
    *,
    connection,
    metric_id: str,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    updated_at: str,
    row_count: int,
) -> int:
    connection.execute("BEGIN")
    delete_metric_scope_snapshot(connection, metric_id=metric_id, scope_key=scope_key)
    return insert_metric_snapshot(
        connection,
        metric_id=metric_id,
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        row_count=row_count,
        updated_at=updated_at,
    )


def _finish_metric_scope_replace(
    *,
    connection,
    snapshot_id: int,
    catalog_row: MetricScopeCatalogRow,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
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
