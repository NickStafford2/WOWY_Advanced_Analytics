from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db


@dataclass(frozen=True)
class MetricCacheEntryState:
    metric_cache_entry_id: int | None
    metric_id: str
    metric_cache_key: str
    build_version: str
    source_fingerprint: str
    row_count: int
    updated_at: str


def load_metric_cache_entry_state(
    metric: str,
    metric_cache_key: str,
) -> MetricCacheEntryState | None:
    initialize_metric_store_db()
    with connect(METRIC_STORE_DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT
                metric_cache_entry_id,
                metric_id,
                metric_cache_key,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            FROM metric_cache_entry
            WHERE metric_id = ? AND metric_cache_key = ?
            """,
            (metric, metric_cache_key),
        ).fetchone()
    if row is None:
        return None
    return MetricCacheEntryState(
        metric_cache_entry_id=cast(int | None, row["metric_cache_entry_id"]),
        metric_id=cast(str, row["metric_id"]),
        metric_cache_key=cast(str, row["metric_cache_key"]),
        build_version=cast(str, row["build_version"]),
        source_fingerprint=cast(str, row["source_fingerprint"]),
        row_count=cast(int, row["row_count"]),
        updated_at=cast(str, row["updated_at"]),
    )
