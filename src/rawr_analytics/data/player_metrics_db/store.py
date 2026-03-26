from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from rawr_analytics.data.player_metrics_db.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
)
from rawr_analytics.data.player_metrics_db.schema import _connect, initialize_player_metrics_db
from rawr_analytics.data.player_metrics_db.validation import (
    _validate_metric_full_span_rows,
    _validate_metric_rows,
    _validate_metric_scope_catalog_row,
)


def replace_metric_rows(
    db_path: Path,
    *,
    metric: str,
    scope_key: str,
    metric_label: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[PlayerSeasonMetricRow],
) -> None:
    initialize_player_metrics_db(db_path)
    _validate_metric_rows(
        metric=metric,
        scope_key=scope_key,
        metric_label=metric_label,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    updated_at = datetime.now(UTC).isoformat()

    with _connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM metric_player_season_values WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.executemany(
            """
            INSERT INTO metric_player_season_values (
                metric,
                metric_label,
                scope_key,
                team_filter,
                season_type,
                season,
                player_id,
                player_name,
                value,
                sample_size,
                secondary_sample_size,
                average_minutes,
                total_minutes,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric,
                    row.metric_label,
                    row.scope_key,
                    row.team_filter,
                    row.season_type,
                    row.season,
                    row.player_id,
                    row.player_name,
                    row.value,
                    row.sample_size,
                    row.secondary_sample_size,
                    row.average_minutes,
                    row.total_minutes,
                    json.dumps(row.details or {}, sort_keys=True),
                )
                for row in rows
            ],
        )
        connection.execute(
            """
            INSERT INTO metric_store_metadata_v2 (
                metric,
                scope_key,
                metric_label,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric, scope_key) DO UPDATE SET
                metric_label = excluded.metric_label,
                build_version = excluded.build_version,
                source_fingerprint = excluded.source_fingerprint,
                row_count = excluded.row_count,
                updated_at = excluded.updated_at
            """,
            (
                metric,
                scope_key,
                metric_label,
                build_version,
                source_fingerprint,
                len(rows),
                updated_at,
            ),
        )
        connection.commit()


def replace_metric_scope_store(
    db_path: Path,
    *,
    metric: str,
    scope_key: str,
    metric_label: str,
    build_version: str,
    source_fingerprint: str,
    rows: list[PlayerSeasonMetricRow],
    catalog_row: MetricScopeCatalogRow,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    initialize_player_metrics_db(db_path)
    _validate_metric_rows(
        metric=metric,
        scope_key=scope_key,
        metric_label=metric_label,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
    )
    _validate_metric_scope_catalog_row(catalog_row)
    _validate_metric_full_span_rows(
        metric=metric,
        scope_key=scope_key,
        series_rows=series_rows,
        point_rows=point_rows,
    )
    updated_at = datetime.now(UTC).isoformat()

    with _connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM metric_full_span_points WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_full_span_series WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_scope_catalog WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_store_metadata_v2 WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_player_season_values WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.executemany(
            """
            INSERT INTO metric_player_season_values (
                metric,
                metric_label,
                scope_key,
                team_filter,
                season_type,
                season,
                player_id,
                player_name,
                value,
                sample_size,
                secondary_sample_size,
                average_minutes,
                total_minutes,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric,
                    row.metric_label,
                    row.scope_key,
                    row.team_filter,
                    row.season_type,
                    row.season,
                    row.player_id,
                    row.player_name,
                    row.value,
                    row.sample_size,
                    row.secondary_sample_size,
                    row.average_minutes,
                    row.total_minutes,
                    json.dumps(row.details or {}, sort_keys=True),
                )
                for row in rows
            ],
        )
        connection.execute(
            """
            INSERT INTO metric_store_metadata_v2 (
                metric,
                scope_key,
                metric_label,
                build_version,
                source_fingerprint,
                row_count,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                metric,
                scope_key,
                metric_label,
                build_version,
                source_fingerprint,
                len(rows),
                updated_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO metric_scope_catalog (
                metric,
                scope_key,
                metric_label,
                team_filter,
                season_type,
                available_seasons_json,
                available_teams_json,
                full_span_start_season,
                full_span_end_season,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                catalog_row.metric,
                catalog_row.scope_key,
                catalog_row.metric_label,
                catalog_row.team_filter,
                catalog_row.season_type,
                json.dumps(catalog_row.available_seasons),
                json.dumps(catalog_row.available_teams),
                catalog_row.full_span_start_season,
                catalog_row.full_span_end_season,
                catalog_row.updated_at,
            ),
        )
        connection.executemany(
            """
            INSERT INTO metric_full_span_series (
                metric,
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
                    row.metric,
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
                metric,
                scope_key,
                player_id,
                season,
                value
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric,
                    row.scope_key,
                    row.player_id,
                    row.season,
                    row.value,
                )
                for row in point_rows
            ],
        )
        connection.commit()


def clear_metric_scope_store(
    db_path: Path,
    *,
    metric: str,
    scope_key: str,
) -> None:
    initialize_player_metrics_db(db_path)
    with _connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM metric_full_span_points WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_full_span_series WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_scope_catalog WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_store_metadata_v2 WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_player_season_values WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.commit()


def replace_metric_scope_catalog_row(
    db_path: Path,
    *,
    row: MetricScopeCatalogRow,
) -> None:
    initialize_player_metrics_db(db_path)
    _validate_metric_scope_catalog_row(row)
    with _connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO metric_scope_catalog (
                metric,
                scope_key,
                metric_label,
                team_filter,
                season_type,
                available_seasons_json,
                available_teams_json,
                full_span_start_season,
                full_span_end_season,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric, scope_key) DO UPDATE SET
                metric_label = excluded.metric_label,
                team_filter = excluded.team_filter,
                season_type = excluded.season_type,
                available_seasons_json = excluded.available_seasons_json,
                available_teams_json = excluded.available_teams_json,
                full_span_start_season = excluded.full_span_start_season,
                full_span_end_season = excluded.full_span_end_season,
                updated_at = excluded.updated_at
            """,
            (
                row.metric,
                row.scope_key,
                row.metric_label,
                row.team_filter,
                row.season_type,
                json.dumps(row.available_seasons),
                json.dumps(row.available_teams),
                row.full_span_start_season,
                row.full_span_end_season,
                row.updated_at,
            ),
        )
        connection.commit()


def replace_metric_full_span_rows(
    db_path: Path,
    *,
    metric: str,
    scope_key: str,
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> None:
    initialize_player_metrics_db(db_path)
    _validate_metric_full_span_rows(
        metric=metric,
        scope_key=scope_key,
        series_rows=series_rows,
        point_rows=point_rows,
    )
    with _connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            "DELETE FROM metric_full_span_points WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.execute(
            "DELETE FROM metric_full_span_series WHERE metric = ? AND scope_key = ?",
            (metric, scope_key),
        )
        connection.executemany(
            """
            INSERT INTO metric_full_span_series (
                metric,
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
                    row.metric,
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
                metric,
                scope_key,
                player_id,
                season,
                value
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    row.metric,
                    row.scope_key,
                    row.player_id,
                    row.season,
                    row.value,
                )
                for row in point_rows
            ],
        )
        connection.commit()
