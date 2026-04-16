from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db

DEFAULT_RETAINED_METRIC_CACHE_KEY_LIMIT = 10


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


def list_retained_metric_cache_keys(
    *,
    metric_id: str,
    pinned_metric_cache_keys: list[str],
    limit: int = DEFAULT_RETAINED_METRIC_CACHE_KEY_LIMIT,
) -> list[str]:
    initialize_metric_store_db()
    if limit <= 0:
        return []

    retained_keys: list[str] = []
    seen_keys: set[str] = set()
    for metric_cache_key in pinned_metric_cache_keys:
        if metric_cache_key in seen_keys:
            continue
        retained_keys.append(metric_cache_key)
        seen_keys.add(metric_cache_key)
        if len(retained_keys) >= limit:
            return retained_keys

    remaining_slots = limit - len(retained_keys)
    if remaining_slots <= 0:
        return retained_keys

    for metric_cache_key in _list_most_used_metric_cache_keys(
        metric_id=metric_id,
        limit=remaining_slots,
        excluded_metric_cache_keys=seen_keys,
    ):
        retained_keys.append(metric_cache_key)
        seen_keys.add(metric_cache_key)

    return retained_keys


def _list_most_used_metric_cache_keys(
    *,
    metric_id: str,
    limit: int,
    excluded_metric_cache_keys: set[str],
) -> list[str]:
    if limit <= 0:
        return []

    query = """
        SELECT metric_cache_key
        FROM metric_cache_query_usage
        WHERE metric_id = ?
    """
    params: list[object] = [metric_id]

    excluded_keys = sorted(excluded_metric_cache_keys)
    if excluded_keys:
        query += f" AND metric_cache_key NOT IN ({','.join('?' for _ in excluded_keys)})"
        params.extend(excluded_keys)

    query += """
        ORDER BY query_count DESC, last_requested_at DESC, metric_cache_key ASC
        LIMIT ?
    """
    params.append(limit)

    with connect(METRIC_STORE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [cast(str, row["metric_cache_key"]) for row in rows]


__all__ = [
    "DEFAULT_RETAINED_METRIC_CACHE_KEY_LIMIT",
    "list_retained_metric_cache_keys",
    "record_metric_cache_query",
]
