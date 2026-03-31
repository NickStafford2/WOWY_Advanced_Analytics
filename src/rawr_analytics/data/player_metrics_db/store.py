from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from rawr_analytics.data.constants import DB_PATH
from rawr_analytics.data.player_metrics_db._tables import metric_values_table
from rawr_analytics.data.player_metrics_db._validation import (
    validate_metric_full_span_rows,
    validate_metric_rows,
    validate_metric_scope_catalog_row,
)
from rawr_analytics.data.player_metrics_db.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
)
from rawr_analytics.data.player_metrics_db.schema import connect, initialize_player_metrics_db


def _replace_metric_rows(
    db_path: Path,
    *,
    metric_id: str,
    scope_key: str,
    label: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[PlayerSeasonMetricRow],
) -> None:
    initialize_player_metrics_db()
    validate_metric_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        label=label,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    updated_at = datetime.now(UTC).isoformat()

    with connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            f"DELETE FROM {metric_values_table(metric_id)} WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        _insert_metric_rows(connection, rows)
        connection.execute(
            """
            INSERT INTO metric_store_metadata_v2 (
                metric_id,
                scope_key,
                label,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_id, scope_key) DO UPDATE SET
                label = excluded.label,
                build_version = excluded.build_version,
                source_fingerprint = excluded.source_fingerprint,
                row_count = excluded.row_count,
                updated_at = excluded.updated_at
            """,
            (
                metric_id,
                scope_key,
                label,
                build_version,
                source_fingerprint,
                len(rows),
                updated_at,
            ),
        )
        connection.commit()


def replace_metric_scope_store(
    *,
    metric_id: str,
    scope_key: str,
    label: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[PlayerSeasonMetricRow],
    catalog_row: MetricScopeCatalogRow,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    initialize_player_metrics_db()
    validate_metric_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        label=label,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    validate_metric_scope_catalog_row(catalog_row)
    validate_metric_full_span_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        series_rows=series_rows,
        point_rows=point_rows,
    )
    updated_at = datetime.now(UTC).isoformat()

    with connect(DB_PATH) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM metric_full_span_points WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_full_span_series WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_scope_catalog WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_store_metadata_v2 WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        connection.execute(
            f"DELETE FROM {metric_values_table(metric_id)} WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        _insert_metric_rows(connection, rows)
        connection.execute(
            """
            INSERT INTO metric_store_metadata_v2 (
                metric_id,
                scope_key,
                label,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                metric_id,
                scope_key,
                label,
                build_version,
                source_fingerprint,
                len(rows),
                updated_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO metric_scope_catalog (
                metric_id,
                scope_key,
                label,
                team_filter,
                season_type,
                available_season_ids_json,
                available_team_ids_json,
                full_span_start_season_id,
                full_span_end_season_id,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                catalog_row.metric_id,
                catalog_row.scope_key,
                catalog_row.label,
                catalog_row.team_filter,
                catalog_row.season_type,
                json.dumps(catalog_row.available_season_ids),
                json.dumps(catalog_row.available_team_ids),
                catalog_row.full_span_start_season_id,
                catalog_row.full_span_end_season_id,
                catalog_row.updated_at,
            ),
        )
        connection.executemany(
            """
            INSERT INTO metric_full_span_series (
                metric_id,
                scope_key,
                player_id,
                player_name,
                span_average_value,
                season_count,
                rank_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric_id,
                    row.scope_key,
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
                metric_id,
                scope_key,
                player_id,
                season_id,
                value
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric_id,
                    row.scope_key,
                    row.player_id,
                    row.season_id,
                    row.value,
                )
                for row in point_rows
            ],
        )
        connection.commit()


def clear_metric_scope_store(
    metric: str,
    scope_key: str,
) -> None:
    initialize_player_metrics_db()
    with connect(DB_PATH) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM metric_full_span_points WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_full_span_series WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_scope_catalog WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_store_metadata_v2 WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            f"DELETE FROM {metric_values_table(metric)} WHERE metric_id = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.commit()


def _insert_metric_rows(
    connection,
    rows: list[PlayerSeasonMetricRow],
) -> None:
    if not rows:
        return
    metric_id = rows[0].metric_id
    if metric_id == "rawr":
        connection.executemany(
            """
            INSERT INTO rawr_player_season_values (
                metric_id,
                scope_key,
                team_filter,
                season_type,
                season_id,
                player_id,
                player_name,
                coefficient,
                games,
                average_minutes,
                total_minutes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric_id,
                    row.scope_key,
                    row.team_filter,
                    row.season_type,
                    row.season_id,
                    row.player_id,
                    row.player_name,
                    row.value,
                    _int_detail(row, "games"),
                    row.average_minutes,
                    row.total_minutes,
                )
                for row in rows
            ],
        )
        return
    if metric_id in {"wowy", "wowy-shrunk"}:
        connection.executemany(
            """
            INSERT INTO wowy_player_season_values (
                metric_id,
                scope_key,
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric_id,
                    row.scope_key,
                    row.team_filter,
                    row.season_type,
                    row.season_id,
                    row.player_id,
                    row.player_name,
                    row.value,
                    _int_detail(row, "games_with"),
                    _int_detail(row, "games_without"),
                    _float_detail(row, "avg_margin_with"),
                    _float_detail(row, "avg_margin_without"),
                    row.average_minutes,
                    row.total_minutes,
                    _require_details(row).get("raw_wowy_score"),
                )
                for row in rows
            ],
        )
        return
    raise ValueError(f"Unknown metric row batch {metric_id!r}")


def _require_details(row: PlayerSeasonMetricRow) -> dict[str, object]:
    assert row.details is not None
    return row.details


def _int_detail(row: PlayerSeasonMetricRow, key: str) -> int:
    value = _require_details(row)[key]
    assert isinstance(value, int)
    return value


def _float_detail(row: PlayerSeasonMetricRow, key: str) -> float:
    value = _require_details(row)[key]
    assert isinstance(value, (int, float))
    return float(value)


def _replace_metric_scope_catalog_row(
    db_path: Path,
    *,
    row: MetricScopeCatalogRow,
) -> None:
    initialize_player_metrics_db()
    validate_metric_scope_catalog_row(row)
    with connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO metric_scope_catalog (
                metric_id,
                scope_key,
                label,
                team_filter,
                season_type,
                available_season_ids_json,
                available_team_ids_json,
                full_span_start_season_id,
                full_span_end_season_id,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_id, scope_key) DO UPDATE SET
                label = excluded.label,
                team_filter = excluded.team_filter,
                season_type = excluded.season_type,
                available_season_ids_json = excluded.available_season_ids_json,
                available_team_ids_json = excluded.available_team_ids_json,
                full_span_start_season_id = excluded.full_span_start_season_id,
                full_span_end_season_id = excluded.full_span_end_season_id,
                updated_at = excluded.updated_at
            """,
            (
                row.metric_id,
                row.scope_key,
                row.label,
                row.team_filter,
                row.season_type,
                json.dumps(row.available_season_ids),
                json.dumps(row.available_team_ids),
                row.full_span_start_season_id,
                row.full_span_end_season_id,
                row.updated_at,
            ),
        )
        connection.commit()


def _replace_metric_full_span_rows(
    db_path: Path,
    *,
    metric_id: str,
    scope_key: str,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    initialize_player_metrics_db()
    validate_metric_full_span_rows(
        metric_id=metric_id,
        scope_key=scope_key,
        series_rows=series_rows,
        point_rows=point_rows,
    )
    with connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM metric_full_span_points WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_full_span_series WHERE metric_id = ? AND scope_key = ?",
            (metric_id, scope_key),
        )
        connection.executemany(
            """
            INSERT INTO metric_full_span_series (
                metric_id,
                scope_key,
                player_id,
                player_name,
                span_average_value,
                season_count,
                rank_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric_id,
                    row.scope_key,
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
                metric_id,
                scope_key,
                player_id,
                season_id,
                value
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric_id,
                    row.scope_key,
                    row.player_id,
                    row.season_id,
                    row.value,
                )
                for row in point_rows
            ],
        )
        connection.commit()
