from __future__ import annotations

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._catalog import MetricCacheCatalogRow
from rawr_analytics.data.metric_store._sql_writes import (
    delete_metric_cache_rows,
    insert_metric_cache_seasons,
    insert_metric_cache_teams,
    insert_metric_cache_entry,
    insert_rawr_rows,
    insert_wowy_rows,
)
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
)
from rawr_analytics.data.metric_store._validation import (
    validate_metric_cache_catalog_row,
)
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db


def replace_rawr_metric_cache(
    *,
    metric_cache_key: str,
    build_version: str,
    source_fingerprint: str,
    updated_at: str,
    catalog_row: MetricCacheCatalogRow,
    rows: list[RawrPlayerSeasonValueRow],
    row_count: int,
) -> None:
    _validate_metric_cache_row_set(
        metric_cache_key=metric_cache_key,
        updated_at=updated_at,
        catalog_row=catalog_row,
    )
    with connect(METRIC_STORE_DB_PATH) as connection:
        metric_cache_entry_id = _begin_metric_cache_replace(
            connection=connection,
            metric_id="rawr",
            metric_cache_key=metric_cache_key,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            updated_at=updated_at,
            row_count=row_count,
        )
        insert_rawr_rows(connection, rows, metric_cache_entry_id)
        _finish_metric_cache_replace(
            connection=connection,
            catalog_row=catalog_row,
        )


def replace_wowy_metric_cache(
    *,
    metric_id: str,
    metric_cache_key: str,
    build_version: str,
    source_fingerprint: str,
    updated_at: str,
    catalog_row: MetricCacheCatalogRow,
    rows: list[WowyPlayerSeasonValueRow],
    row_count: int,
) -> None:
    _validate_metric_cache_row_set(
        metric_cache_key=metric_cache_key,
        updated_at=updated_at,
        catalog_row=catalog_row,
    )
    with connect(METRIC_STORE_DB_PATH) as connection:
        metric_cache_entry_id = _begin_metric_cache_replace(
            connection=connection,
            metric_id=metric_id,
            metric_cache_key=metric_cache_key,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            updated_at=updated_at,
            row_count=row_count,
        )
        insert_wowy_rows(connection, rows, metric_cache_entry_id)
        _finish_metric_cache_replace(
            connection=connection,
            catalog_row=catalog_row,
        )


def _validate_metric_cache_row_set(
    *,
    metric_cache_key: str,
    updated_at: str,
    catalog_row: MetricCacheCatalogRow,
) -> None:
    initialize_metric_store_db()
    validate_metric_cache_catalog_row(catalog_row)
    if catalog_row.updated_at != updated_at:
        raise ValueError("Metric cache writes require one shared updated_at timestamp")


def _begin_metric_cache_replace(
    *,
    connection,
    metric_id: str,
    metric_cache_key: str,
    build_version: str,
    source_fingerprint: str,
    updated_at: str,
    row_count: int,
) -> int:
    connection.execute("BEGIN")
    delete_metric_cache_rows(
        connection,
        metric_id=metric_id,
        metric_cache_key=metric_cache_key,
    )
    return insert_metric_cache_entry(
        connection,
        metric_id=metric_id,
        metric_cache_key=metric_cache_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        row_count=row_count,
        updated_at=updated_at,
    )


def _finish_metric_cache_replace(
    *,
    connection,
    catalog_row: MetricCacheCatalogRow,
) -> None:
    connection.execute(
        """
        INSERT INTO metric_cache_catalog (
            metric_id,
            metric_cache_key,
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
            catalog_row.metric_cache_key,
            catalog_row.label,
            catalog_row.team_filter,
            catalog_row.season_type,
            catalog_row.full_span_start_season_id,
            catalog_row.full_span_end_season_id,
            catalog_row.updated_at,
        ),
    )
    insert_metric_cache_seasons(connection, catalog_row)
    insert_metric_cache_teams(connection, catalog_row)
    connection.commit()
