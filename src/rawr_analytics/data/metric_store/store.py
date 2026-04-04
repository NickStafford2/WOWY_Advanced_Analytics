from __future__ import annotations

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._sql_writes import (
    delete_metric_full_span_rows,
    delete_metric_rows,
)
from rawr_analytics.data.metric_store.schema import connect, initialize_player_metrics_db


def clear_metric_scope_store(
    metric: str,
    scope_key: str,
) -> None:
    initialize_player_metrics_db()
    with connect(METRIC_STORE_DB_PATH) as connection:
        connection.execute("BEGIN")
        delete_metric_full_span_rows(connection, metric_id=metric, scope_key=scope_key)
        connection.execute(
            "DELETE FROM metric_scope_catalog WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_scope_season WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_scope_team WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        delete_metric_rows(connection, metric_id=metric, scope_key=scope_key)
        connection.execute(
            "DELETE FROM metric_snapshot WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.commit()


__all__ = [
    "clear_metric_scope_store",
]
