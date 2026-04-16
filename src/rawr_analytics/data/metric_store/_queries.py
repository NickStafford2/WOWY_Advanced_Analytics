from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._catalog import MetricCacheCatalogRow
from rawr_analytics.data.metric_store.schema import connect, initialize_metric_store_db


@dataclass(frozen=True)
class MetricSnapshotState:
    snapshot_id: int | None
    metric_id: str
    metric_cache_key: str
    build_version: str
    source_fingerprint: str
    row_count: int
    updated_at: str


def load_metric_cache_entry_state(
    metric: str,
    metric_cache_key: str,
) -> MetricSnapshotState | None:
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
    return MetricSnapshotState(
        snapshot_id=cast(int | None, row["metric_cache_entry_id"]),
        metric_id=cast(str, row["metric_id"]),
        metric_cache_key=cast(str, row["metric_cache_key"]),
        build_version=cast(str, row["build_version"]),
        source_fingerprint=cast(str, row["source_fingerprint"]),
        row_count=cast(int, row["row_count"]),
        updated_at=cast(str, row["updated_at"]),
    )


def load_metric_cache_catalog_row(
    metric: str,
    metric_cache_key: str,
) -> MetricCacheCatalogRow | None:
    initialize_metric_store_db()
    with connect(METRIC_STORE_DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT
                metric_id,
                metric_cache_key,
                label,
                team_filter,
                season_type,
                full_span_start_season_id,
                full_span_end_season_id,
                updated_at
            FROM metric_cache_catalog
            WHERE metric_id = ? AND metric_cache_key = ?
            """,
            (metric, metric_cache_key),
        ).fetchone()
        if row is None:
            return None
        team_rows = connection.execute(
            """
            SELECT team_id
            FROM metric_cache_team
            WHERE metric_id = ? AND metric_cache_key = ?
            ORDER BY team_id
            """,
            (metric, metric_cache_key),
        ).fetchall()
        season_rows = connection.execute(
            """
            SELECT season_id
            FROM metric_cache_season
            WHERE metric_id = ? AND metric_cache_key = ?
            ORDER BY season_id
            """,
            (metric, metric_cache_key),
        ).fetchall()
    return MetricCacheCatalogRow(
        metric_id=cast(str, row["metric_id"]),
        metric_cache_key=cast(str, row["metric_cache_key"]),
        label=cast(str, row["label"]),
        team_filter=cast(str, row["team_filter"]),
        season_type=cast(str, row["season_type"]),
        available_season_ids=[cast(str, season_row["season_id"]) for season_row in season_rows],
        available_team_ids=[cast(int, team_row["team_id"]) for team_row in team_rows],
        full_span_start_season_id=cast(str | None, row["full_span_start_season_id"]),
        full_span_end_season_id=cast(str | None, row["full_span_end_season_id"]),
        updated_at=cast(str, row["updated_at"]),
    )
