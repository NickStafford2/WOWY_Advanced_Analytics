from __future__ import annotations

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._sql_writes import (
    delete_metric_cache_rows,
    delete_metric_cache_rows_except,
    insert_metric_cache_entry,
    insert_rawr_rows,
    insert_wowy_rows,
)
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
)
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db


def replace_rawr_metric_cache(
    *,
    metric_cache_key: str,
    build_version: str,
    source_fingerprint: str,
    updated_at: str,
    rows: list[RawrPlayerSeasonValueRow],
    row_count: int,
) -> None:
    _validate_metric_cache_row_set(
        metric_cache_key=metric_cache_key,
        updated_at=updated_at,
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
        connection.commit()


def replace_wowy_metric_cache(
    *,
    metric_id: str,
    metric_cache_key: str,
    build_version: str,
    source_fingerprint: str,
    updated_at: str,
    rows: list[WowyPlayerSeasonValueRow],
    row_count: int,
) -> None:
    _validate_metric_cache_row_set(
        metric_cache_key=metric_cache_key,
        updated_at=updated_at,
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
        connection.commit()


def _validate_metric_cache_row_set(
    *,
    metric_cache_key: str,
    updated_at: str,
) -> None:
    initialize_metric_store_db()
    if not updated_at:
        raise ValueError("Metric cache writes require updated_at")


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


def prune_metric_caches(
    *,
    metric_id: str,
    retained_metric_cache_keys: list[str],
) -> None:
    initialize_metric_store_db()
    with connect(METRIC_STORE_DB_PATH) as connection:
        connection.execute("BEGIN")
        delete_metric_cache_rows_except(
            connection,
            metric_id=metric_id,
            retained_metric_cache_keys=retained_metric_cache_keys,
        )
        connection.commit()
