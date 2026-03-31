from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from rawr_analytics.data.player_metrics_db._validation import (
    _validate_metric_full_span_rows,
    _validate_metric_rows,
    _validate_metric_scope_catalog_row,
)
from rawr_analytics.data.player_metrics_db.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
)

IssueT = TypeVar("IssueT")
IssueFactory = Callable[[str, str, str], IssueT]


@dataclass(frozen=True)
class MetricStoreAuditMetadata:
    label: str
    build_version: str
    source_fingerprint: str
    row_count: int


@dataclass(frozen=True)
class MetricStoreAuditState:
    metric_row_groups: dict[tuple[str, str], list[PlayerSeasonMetricRow]]
    metadata_rows: dict[tuple[str, str], MetricStoreAuditMetadata]
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow]
    full_span_groups: dict[
        tuple[str, str], tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]
    ]


def audit_metric_store_tables(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> MetricStoreAuditState:
    metric_row_groups, metadata_rows = _audit_metric_player_season_values_table(
        connection,
        issues,
        issue_factory=issue_factory,
    )
    catalog_rows = _audit_metric_scope_catalog_table(
        connection,
        issues,
        issue_factory=issue_factory,
    )
    full_span_groups = _audit_metric_full_span_tables(
        connection,
        issues,
        issue_factory=issue_factory,
    )
    return MetricStoreAuditState(
        metric_row_groups=metric_row_groups,
        metadata_rows=metadata_rows,
        catalog_rows=catalog_rows,
        full_span_groups=full_span_groups,
    )


def _audit_metric_player_season_values_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> tuple[
    dict[tuple[str, str], list[PlayerSeasonMetricRow]],
    dict[tuple[str, str], MetricStoreAuditMetadata],
]:
    metadata_rows = connection.execute(
        """
        SELECT
            metric_id,
            scope_key,
            label,
            build_version,
            source_fingerprint,
            row_count
        FROM metric_store_metadata_v2
        """
    ).fetchall()
    metadata_by_key = {
        (row["metric_id"], row["scope_key"]): MetricStoreAuditMetadata(
            label=row["label"],
            build_version=row["build_version"],
            source_fingerprint=row["source_fingerprint"],
            row_count=row["row_count"],
        )
        for row in metadata_rows
    }

    rows = connection.execute(
        """
        SELECT
            metric_id,
            scope_key,
            team_filter,
            season_type,
            season_id,
            player_id,
            player_name,
            value,
            sample_size,
            secondary_sample_size,
            average_minutes,
            total_minutes,
            details_json
        FROM metric_player_season_values
        ORDER BY metric_id, scope_key, season_id, player_id
        """
    ).fetchall()

    groups: dict[tuple[str, str], list[PlayerSeasonMetricRow]] = defaultdict(list)
    for row in rows:
        groups[(row["metric_id"], row["scope_key"])].append(
            PlayerSeasonMetricRow(
                metric_id=row["metric_id"],
                scope_key=row["scope_key"],
                team_filter=row["team_filter"],
                season_type=row["season_type"],
                season_id=row["season_id"],
                player_id=row["player_id"],
                player_name=row["player_name"],
                value=row["value"],
                sample_size=row["sample_size"],
                secondary_sample_size=row["secondary_sample_size"],
                average_minutes=row["average_minutes"],
                total_minutes=row["total_minutes"],
                details=json.loads(row["details_json"]),
            )
        )

    for key, group_rows in groups.items():
        metric, scope_key = key
        metadata_row = metadata_by_key.get(
            key,
            MetricStoreAuditMetadata(
                label="missing-metadata",
                build_version="missing-metadata",
                source_fingerprint="missing-metadata",
                row_count=-1,
            ),
        )
        try:
            _validate_metric_rows(
                metric_id=metric,
                scope_key=scope_key,
                label=metadata_row.label,
                build_version=metadata_row.build_version,
                source_fingerprint=metadata_row.source_fingerprint,
                rows=group_rows,
            )
        except ValueError as exc:
            issues.append(
                issue_factory(
                    "metric_player_season_values",
                    f"metric={metric!r},scope_key={scope_key!r}",
                    str(exc),
                )
            )

    return groups, metadata_by_key


def _audit_metric_scope_catalog_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> dict[tuple[str, str], MetricScopeCatalogRow]:
    rows = connection.execute(
        """
        SELECT
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
        FROM metric_scope_catalog
        ORDER BY metric_id, scope_key
        """
    ).fetchall()
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow] = {}
    for row in rows:
        catalog_row = MetricScopeCatalogRow(
            metric_id=row["metric_id"],
            scope_key=row["scope_key"],
            label=row["label"],
            team_filter=row["team_filter"],
            season_type=row["season_type"],
            available_season_ids=json.loads(row["available_season_ids_json"]),
            available_team_ids=json.loads(row["available_team_ids_json"]),
            full_span_start_season_id=row["full_span_start_season_id"],
            full_span_end_season_id=row["full_span_end_season_id"],
            updated_at=row["updated_at"],
        )
        key = (catalog_row.metric_id, catalog_row.scope_key)
        catalog_rows[key] = catalog_row
        try:
            _validate_metric_scope_catalog_row(catalog_row)
        except ValueError as exc:
            issues.append(
                issue_factory(
                    "metric_scope_catalog",
                    f"metric={catalog_row.metric_id!r},scope_key={catalog_row.scope_key!r}",
                    str(exc),
                )
            )
    return catalog_rows


def _audit_metric_full_span_tables(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> dict[tuple[str, str], tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]]:
    series_rows = connection.execute(
        """
        SELECT
            metric_id,
            scope_key,
            player_id,
            player_name,
            span_average_value,
            season_count,
            rank_order
        FROM metric_full_span_series
        ORDER BY metric_id, scope_key, rank_order, player_id
        """
    ).fetchall()
    point_rows = connection.execute(
        """
        SELECT
            metric_id,
            scope_key,
            player_id,
            season_id,
            value
        FROM metric_full_span_points
        ORDER BY metric_id, scope_key, player_id, season_id
        """
    ).fetchall()

    series_groups: dict[tuple[str, str], list[MetricFullSpanSeriesRow]] = defaultdict(list)
    point_groups: dict[tuple[str, str], list[MetricFullSpanPointRow]] = defaultdict(list)

    for row in series_rows:
        series_groups[(row["metric_id"], row["scope_key"])].append(
            MetricFullSpanSeriesRow(
                metric_id=row["metric_id"],
                scope_key=row["scope_key"],
                player_id=row["player_id"],
                player_name=row["player_name"],
                span_average_value=row["span_average_value"],
                season_count=row["season_count"],
                rank_order=row["rank_order"],
            )
        )
    for row in point_rows:
        point_groups[(row["metric_id"], row["scope_key"])].append(
            MetricFullSpanPointRow(
                metric_id=row["metric_id"],
                scope_key=row["scope_key"],
                player_id=row["player_id"],
                season_id=row["season_id"],
                value=row["value"],
            )
        )

    groups: dict[
        tuple[str, str], tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]
    ] = {}
    for key in sorted(set(series_groups) | set(point_groups)):
        series = series_groups.get(key, [])
        points = point_groups.get(key, [])
        groups[key] = (series, points)
        try:
            _validate_metric_full_span_rows(
                metric_id=key[0],
                scope_key=key[1],
                series_rows=series,
                point_rows=points,
            )
        except ValueError as exc:
            issues.append(
                issue_factory(
                    "metric_full_span",
                    f"metric={key[0]!r},scope_key={key[1]!r}",
                    str(exc),
                )
            )

    return groups
