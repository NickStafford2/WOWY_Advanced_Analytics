from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db


def record_metric_cache_query(
    *,
    metric_id: str,
    metric_cache_key: str,
) -> None:
    initialize_metric_store_db()
    requested_at = datetime.now(UTC).isoformat()
    with connect(METRIC_STORE_DB_PATH) as connection:
        connection.execute(
            """
            INSERT INTO metric_cache_query_usage (
                metric_id,
                metric_cache_key,
                query_count,
                last_requested_at
            )
            VALUES (?, ?, 1, ?)
            ON CONFLICT(metric_id, metric_cache_key) DO UPDATE SET
                query_count = metric_cache_query_usage.query_count + 1,
                last_requested_at = excluded.last_requested_at
            """,
            (metric_id, metric_cache_key, requested_at),
        )
        connection.commit()


def list_metric_cache_keys_by_usage(
    *,
    metric_id: str,
) -> list[str]:
    initialize_metric_store_db()
    with connect(METRIC_STORE_DB_PATH) as connection:
        rows = connection.execute(
            """
        SELECT metric_cache_key
        FROM metric_cache_query_usage
        WHERE metric_id = ?
        ORDER BY query_count DESC, last_requested_at DESC, metric_cache_key ASC
            """,
            (metric_id,),
        ).fetchall()
    return [cast(str, row["metric_cache_key"]) for row in rows]


__all__ = [
    "list_metric_cache_keys_by_usage",
    "record_metric_cache_query",
]
