from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, TypeVar

from rawr_analytics.data.player_metrics_db.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
)
from rawr_analytics.data.player_metrics_db.validation import (
    _validate_metric_full_span_rows,
    _validate_metric_rows,
    _validate_metric_scope_catalog_row,
)

IssueT = TypeVar("IssueT")
IssueFactory = Callable[[str, str, str], IssueT]


@dataclass(frozen=True)
class MetricStoreAuditMetadata:
    metric_label: str
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
            metric,
            scope_key,
            metric_label,
            build_version,
            source_fingerprint,
            row_count
        FROM metric_store_metadata_v2
        """
    ).fetchall()
    metadata_by_key = {
        (row["metric"], row["scope_key"]): MetricStoreAuditMetadata(
            metric_label=row["metric_label"],
            build_version=row["build_version"],
            source_fingerprint=row["source_fingerprint"],
            row_count=row["row_count"],
        )
        for row in metadata_rows
    }

    rows = connection.execute(
        """
        SELECT
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
        FROM metric_player_season_values
        ORDER BY metric, scope_key, season, player_id
        """
    ).fetchall()

    groups: dict[tuple[str, str], list[PlayerSeasonMetricRow]] = defaultdict(list)
    for row in rows:
        groups[(row["metric"], row["scope_key"])].append(
            PlayerSeasonMetricRow(
                metric=row["metric"],
                metric_label=row["metric_label"],
                scope_key=row["scope_key"],
                team_filter=row["team_filter"],
                season_type=row["season_type"],
                season=row["season"],
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
                metric_label=group_rows[0].metric_label,
                build_version="missing-metadata",
                source_fingerprint="missing-metadata",
                row_count=-1,
            ),
        )
        try:
            _validate_metric_rows(
                metric=metric,
                scope_key=scope_key,
                metric_label=metadata_row.metric_label,
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
        FROM metric_scope_catalog
        ORDER BY metric, scope_key
        """
    ).fetchall()
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow] = {}
    for row in rows:
        catalog_row = MetricScopeCatalogRow(
            metric=row["metric"],
            scope_key=row["scope_key"],
            metric_label=row["metric_label"],
            team_filter=row["team_filter"],
            season_type=row["season_type"],
            available_seasons=json.loads(row["available_seasons_json"]),
            available_teams=json.loads(row["available_teams_json"]),
            full_span_start_season=row["full_span_start_season"],
            full_span_end_season=row["full_span_end_season"],
            updated_at=row["updated_at"],
        )
        key = (catalog_row.metric, catalog_row.scope_key)
        catalog_rows[key] = catalog_row
        try:
            _validate_metric_scope_catalog_row(catalog_row)
        except ValueError as exc:
            issues.append(
                issue_factory(
                    "metric_scope_catalog",
                    f"metric={catalog_row.metric!r},scope_key={catalog_row.scope_key!r}",
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
            metric,
            scope_key,
            player_id,
            player_name,
            span_average_value,
            season_count,
            rank_order
        FROM metric_full_span_series
        ORDER BY metric, scope_key, rank_order, player_id
        """
    ).fetchall()
    point_rows = connection.execute(
        """
        SELECT
            metric,
            scope_key,
            player_id,
            season,
            value
        FROM metric_full_span_points
        ORDER BY metric, scope_key, player_id, season
        """
    ).fetchall()

    series_groups: dict[tuple[str, str], list[MetricFullSpanSeriesRow]] = defaultdict(list)
    point_groups: dict[tuple[str, str], list[MetricFullSpanPointRow]] = defaultdict(list)

    for row in series_rows:
        series_groups[(row["metric"], row["scope_key"])].append(
            MetricFullSpanSeriesRow(
                metric=row["metric"],
                scope_key=row["scope_key"],
                player_id=row["player_id"],
                player_name=row["player_name"],
                span_average_value=row["span_average_value"],
                season_count=row["season_count"],
                rank_order=row["rank_order"],
            )
        )
    for row in point_rows:
        point_groups[(row["metric"], row["scope_key"])].append(
            MetricFullSpanPointRow(
                metric=row["metric"],
                scope_key=row["scope_key"],
                player_id=row["player_id"],
                season=row["season"],
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
                metric=key[0],
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
