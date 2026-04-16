from __future__ import annotations

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._sql_writes import delete_metric_scope_snapshot
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db


def clear_metric_scope_store(
    metric: str,
    scope_key: str,
) -> None:
    initialize_metric_store_db()
    with connect(METRIC_STORE_DB_PATH) as connection:
        connection.execute("BEGIN")
        delete_metric_scope_snapshot(connection, metric_id=metric, scope_key=scope_key)
        connection.commit()


__all__ = [
    "clear_metric_scope_store",
]
