from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH
from rawr_analytics.data.metric_store._tables import metric_values_table
from rawr_analytics.data.metric_store._validation import (
    validate_metric_full_span_rows,
    validate_metric_scope_catalog_row,
    validate_rawr_rows,
    validate_wowy_rows,
)
from rawr_analytics.data.metric_store.full_span import (
    build_rawr_full_span_rows,
    build_wowy_full_span_rows,
)
from rawr_analytics.data.metric_store.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
)
from rawr_analytics.data.metric_store.rawr import RawrPlayerSeasonValueRow
from rawr_analytics.data.metric_store.schema import connect, initialize_player_metrics_db
from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow


def replace_rawr_scope_snapshot(
    *,
    scope_key: str,
    label: str,
    team_filter: str,
    season_type: str,
    available_season_ids: list[str],
    available_team_ids: list[int],
    build_version: str,
    source_fingerprint: str,
    rows: list[RawrPlayerSeasonValueRow],
) -> None:
    updated_at = datetime.now(UTC).isoformat()
    _validate_rawr_rows(
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    series_rows, point_rows = build_rawr_full_span_rows(
        rows=rows,
        scope_key=scope_key,
        season_ids=available_season_ids,
    )
    _replace_metric_scope_snapshot(
        metric_id="rawr",
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        catalog_row=_build_metric_scope_catalog_row(
            metric_id="rawr",
            scope_key=scope_key,
            label=label,
            team_filter=team_filter,
            season_type=season_type,
            available_season_ids=available_season_ids,
            available_team_ids=available_team_ids,
            updated_at=updated_at,
        ),
        series_rows=series_rows,
        point_rows=point_rows,
        insert_rows=lambda connection, snapshot_id: _insert_rawr_rows(
            connection,
            rows,
            snapshot_id,
        ),
        row_count=len(rows),
    )


def replace_wowy_scope_snapshot(
    *,
    metric_id: str,
    scope_key: str,
    label: str,
    team_filter: str,
    season_type: str,
    available_season_ids: list[str],
    available_team_ids: list[int],
    build_version: str,
    source_fingerprint: str,
    rows: list[WowyPlayerSeasonValueRow],
) -> None:
    updated_at = datetime.now(UTC).isoformat()
    _validate_wowy_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    series_rows, point_rows = build_wowy_full_span_rows(
        metric_id=metric_id,
        rows=rows,
        scope_key=scope_key,
        season_ids=available_season_ids,
    )
    _replace_metric_scope_snapshot(
        metric_id=metric_id,
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        catalog_row=_build_metric_scope_catalog_row(
            metric_id=metric_id,
            scope_key=scope_key,
            label=label,
            team_filter=team_filter,
            season_type=season_type,
            available_season_ids=available_season_ids,
            available_team_ids=available_team_ids,
            updated_at=updated_at,
        ),
        series_rows=series_rows,
        point_rows=point_rows,
        insert_rows=lambda connection, snapshot_id: _insert_wowy_rows(
            connection,
            rows,
            snapshot_id,
        ),
        row_count=len(rows),
    )


def clear_metric_scope_store(
    metric: str,
    scope_key: str,
) -> None:
    initialize_player_metrics_db()
    with connect(METRIC_STORE_DB_PATH) as connection:
        connection.execute("BEGIN")
        _delete_metric_full_span_rows(connection, metric_id=metric, scope_key=scope_key)
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
        _delete_metric_rows(connection, metric_id=metric, scope_key=scope_key)
        connection.execute(
            "DELETE FROM metric_snapshot WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.commit()


def _replace_metric_scope_snapshot(
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
        _delete_metric_full_span_rows(connection, metric_id=metric_id, scope_key=scope_key)
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
        _delete_metric_rows(connection, metric_id=metric_id, scope_key=scope_key)
        connection.execute(
            "DELETE FROM metric_snapshot WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        snapshot_id = _insert_metric_snapshot(
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
        _insert_metric_scope_seasons(connection, catalog_row)
        _insert_metric_scope_teams(connection, catalog_row)
        _insert_full_span_rows(connection, snapshot_id, series_rows, point_rows)
        connection.commit()


def _delete_metric_full_span_rows(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    scope_key: str,
) -> None:
    params = (metric_id, scope_key)
    connection.execute(
        """
        DELETE FROM metric_full_span_points
        WHERE snapshot_id IN (
            SELECT snapshot_id
            FROM metric_snapshot
            WHERE metric_id = ? AND scope_key = ?
        )
        """,
        params,
    )
    connection.execute(
        """
        DELETE FROM metric_full_span_series
        WHERE snapshot_id IN (
            SELECT snapshot_id
            FROM metric_snapshot
            WHERE metric_id = ? AND scope_key = ?
        )
        """,
        params,
    )


def _delete_metric_value_rows(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    scope_key: str,
) -> None:
    table = metric_values_table(metric_id)
    connection.execute(
        f"""
        DELETE FROM {table}
        WHERE snapshot_id IN (
            SELECT snapshot_id
            FROM metric_snapshot
            WHERE metric_id = ? AND scope_key = ?
        )
        """,
        (metric_id, scope_key),
    )


def _delete_metric_rows(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    scope_key: str,
) -> None:
    _delete_metric_value_rows(connection, metric_id=metric_id, scope_key=scope_key)


def _build_metric_scope_catalog_row(
    *,
    metric_id: str,
    scope_key: str,
    label: str,
    team_filter: str,
    season_type: str,
    available_season_ids: list[str],
    available_team_ids: list[int],
    updated_at: str,
) -> MetricScopeCatalogRow:
    return MetricScopeCatalogRow(
        metric_id=metric_id,
        scope_key=scope_key,
        label=label,
        team_filter=team_filter,
        season_type=season_type,
        available_season_ids=available_season_ids,
        available_team_ids=available_team_ids,
        full_span_start_season_id=available_season_ids[0] if available_season_ids else None,
        full_span_end_season_id=available_season_ids[-1] if available_season_ids else None,
        updated_at=updated_at,
    )


def _insert_metric_snapshot(
    connection: sqlite3.Connection,
    *,
    metric_id: str,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    row_count: int,
    updated_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO metric_snapshot (
            metric_id,
            scope_key,
            build_version,
            source_fingerprint,
            row_count,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            metric_id,
            scope_key,
            build_version,
            source_fingerprint,
            row_count,
            updated_at,
        ),
    )
    assert cursor.lastrowid is not None, "metric_snapshot insert must produce snapshot_id"
    return int(cursor.lastrowid)


def _insert_rawr_rows(
    connection,
    rows: list[RawrPlayerSeasonValueRow],
    snapshot_id: int | None,
) -> None:
    if not rows:
        return
    assert snapshot_id is not None, "rawr snapshot writes require snapshot_id"
    connection.executemany(
        """
        INSERT INTO rawr_player_season_values (
            snapshot_id,
            team_filter,
            season_type,
            season_id,
            player_id,
            player_name,
            coefficient,
            games,
            average_minutes,
            total_minutes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                snapshot_id,
                row.team_filter,
                row.season_type,
                row.value.season_id,
                row.value.player_id,
                row.value.player_name,
                row.value.coefficient,
                row.value.games,
                row.value.average_minutes,
                row.value.total_minutes,
            )
            for row in rows
        ],
    )


def _insert_wowy_rows(
    connection,
    rows: list[WowyPlayerSeasonValueRow],
    snapshot_id: int | None,
) -> None:
    if not rows:
        return
    assert snapshot_id is not None, "wowy snapshot writes require snapshot_id"
    connection.executemany(
        """
        INSERT INTO wowy_player_season_values (
            snapshot_id,
            team_filter,
            season_type,
            season_id,
            player_id,
            player_name,
            value,
            games_with,
            games_without,
            avg_margin_with,
            avg_margin_without,
            average_minutes,
            total_minutes,
            raw_wowy_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                snapshot_id,
                row.team_filter,
                row.season_type,
                row.value.season_id,
                row.value.player_id,
                row.value.player_name,
                row.value.value,
                row.value.games_with,
                row.value.games_without,
                row.value.avg_margin_with,
                row.value.avg_margin_without,
                row.value.average_minutes,
                row.value.total_minutes,
                row.value.raw_wowy_score,
            )
            for row in rows
        ],
    )


def _insert_metric_scope_teams(connection, row: MetricScopeCatalogRow) -> None:
    if not row.available_team_ids:
        return
    connection.executemany(
        """
        INSERT INTO metric_scope_team (
            metric_id,
            scope_key,
            team_id
        ) VALUES (?, ?, ?)
        """,
        [
            (
                row.metric_id,
                row.scope_key,
                team_id,
            )
            for team_id in row.available_team_ids
        ],
    )


def _insert_metric_scope_seasons(connection, row: MetricScopeCatalogRow) -> None:
    if not row.available_season_ids:
        return
    connection.executemany(
        """
        INSERT INTO metric_scope_season (
            metric_id,
            scope_key,
            season_id
        ) VALUES (?, ?, ?)
        """,
        [
            (
                row.metric_id,
                row.scope_key,
                season_id,
            )
            for season_id in row.available_season_ids
        ],
    )


def _insert_full_span_rows(
    connection,
    snapshot_id: int,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    connection.executemany(
        """
        INSERT INTO metric_full_span_series (
            snapshot_id,
            player_id,
            player_name,
            span_average_value,
            season_count,
            rank_order
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                snapshot_id,
                row.player_id,
                row.player_name,
                row.span_average_value,
                row.season_count,
                row.rank_order,
            )
            for row in series_rows
        ],
    )
    connection.executemany(
        """
        INSERT INTO metric_full_span_points (
            snapshot_id,
            player_id,
            season_id,
            value
        ) VALUES (?, ?, ?, ?)
        """,
        [
            (
                snapshot_id,
                row.player_id,
                row.season_id,
                row.value,
            )
            for row in point_rows
        ],
    )


def _validate_rawr_rows(
    *,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[RawrPlayerSeasonValueRow],
) -> None:
    validate_rawr_rows(
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )


def _validate_wowy_rows(
    *,
    metric_id: str,
    scope_key: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[WowyPlayerSeasonValueRow],
) -> None:
    validate_wowy_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
