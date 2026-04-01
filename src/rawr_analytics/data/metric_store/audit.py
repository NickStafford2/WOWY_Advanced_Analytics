from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass

from rawr_analytics.data._validation_issue import ValidationIssue
from rawr_analytics.data.metric_store._validation import (
    validate_metric_full_span_rows,
    validate_metric_scope_catalog_row,
    validate_rawr_rows,
    validate_wowy_rows,
)
from rawr_analytics.data.metric_store.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
)
from rawr_analytics.data.metric_store.rawr import RawrPlayerSeasonValueRow
from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow


@dataclass(frozen=True)
class MetricStoreAuditMetadata:
    source_table: str
    snapshot_id: int | None
    build_version: str
    source_fingerprint: str
    row_count: int


@dataclass(frozen=True)
class MetricStoreAuditState:
    rawr_row_groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]]
    wowy_row_groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]]
    metadata_rows: dict[tuple[str, str], MetricStoreAuditMetadata]
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow]
    scope_season_rows: dict[tuple[str, str], list[str]]
    scope_team_rows: dict[tuple[str, str], list[int]]
    full_span_groups: dict[
        tuple[str, str], tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]
    ]


def audit_metric_store_tables(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> MetricStoreAuditState:
    rawr_row_groups, wowy_row_groups, metadata_rows = _audit_metric_player_season_values_table(
        connection,
        issues,
    )
    catalog_rows, scope_season_rows, scope_team_rows = _audit_metric_scope_catalog_table(
        connection,
        issues,
    )
    full_span_groups = _audit_metric_full_span_tables(
        connection,
        issues,
    )
    return MetricStoreAuditState(
        rawr_row_groups=rawr_row_groups,
        wowy_row_groups=wowy_row_groups,
        metadata_rows=metadata_rows,
        catalog_rows=catalog_rows,
        scope_season_rows=scope_season_rows,
        scope_team_rows=scope_team_rows,
        full_span_groups=full_span_groups,
    )


def _audit_metric_player_season_values_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> tuple[
    dict[tuple[str, str], list[RawrPlayerSeasonValueRow]],
    dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
    dict[tuple[str, str], MetricStoreAuditMetadata],
]:
    snapshot_rows = connection.execute(
        """
        SELECT
            snapshot_id,
            metric_id,
            scope_key,
            build_version,
            source_fingerprint,
            row_count
        FROM metric_snapshot
        """
    ).fetchall()
    metadata_by_key = {
        (row["metric_id"], row["scope_key"]): MetricStoreAuditMetadata(
            source_table="metric_snapshot",
            snapshot_id=row["snapshot_id"],
            build_version=row["build_version"],
            source_fingerprint=row["source_fingerprint"],
            row_count=row["row_count"],
        )
        for row in snapshot_rows
    }
    legacy_metadata_rows = connection.execute(
        """
        SELECT
            metric_id,
            scope_key,
            build_version,
            source_fingerprint,
            row_count
        FROM metric_store_metadata_v2
        """
    ).fetchall()
    for row in legacy_metadata_rows:
        key = (row["metric_id"], row["scope_key"])
        if row["metric_id"] == "rawr":
            continue
        if key in metadata_by_key:
            continue
        metadata_by_key[key] = MetricStoreAuditMetadata(
            source_table="metric_store_metadata_v2",
            snapshot_id=None,
            build_version=row["build_version"],
            source_fingerprint=row["source_fingerprint"],
            row_count=row["row_count"],
        )

    rawr_groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]] = defaultdict(list)
    wowy_groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]] = defaultdict(list)
    _load_rawr_metric_rows(connection, rawr_groups)
    _load_wowy_metric_rows(connection, wowy_groups)

    for key, group_rows in rawr_groups.items():
        metadata_row = metadata_by_key.get(
            key,
            MetricStoreAuditMetadata(
                source_table="metric_snapshot",
                snapshot_id=None,
                build_version="missing-metadata",
                source_fingerprint="missing-metadata",
                row_count=-1,
            ),
        )
        try:
            validate_rawr_rows(
                scope_key=key[1],
                build_version=metadata_row.build_version,
                source_fingerprint=metadata_row.source_fingerprint,
                rows=group_rows,
            )
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    "rawr_player_season_values",
                    f"metric={key[0]!r},scope_key={key[1]!r}",
                    str(exc),
                )
            )

    for key, group_rows in wowy_groups.items():
        metadata_row = metadata_by_key.get(
            key,
            MetricStoreAuditMetadata(
                source_table="metric_store_metadata_v2",
                snapshot_id=None,
                build_version="missing-metadata",
                source_fingerprint="missing-metadata",
                row_count=-1,
            ),
        )
        try:
            validate_wowy_rows(
                metric_id=key[0],
                scope_key=key[1],
                build_version=metadata_row.build_version,
                source_fingerprint=metadata_row.source_fingerprint,
                rows=group_rows,
            )
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    "wowy_player_season_values",
                    f"metric={key[0]!r},scope_key={key[1]!r}",
                    str(exc),
                )
            )

    return rawr_groups, wowy_groups, metadata_by_key


def _load_rawr_metric_rows(
    connection: sqlite3.Connection,
    groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]],
) -> None:
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
            coefficient,
            games,
            average_minutes,
            total_minutes
        FROM rawr_player_season_values
        ORDER BY metric_id, scope_key, season_id, player_id
        """
    ).fetchall()
    for row in rows:
        groups[(row["metric_id"], row["scope_key"])].append(
            RawrPlayerSeasonValueRow(
                metric_id=row["metric_id"],
                scope_key=row["scope_key"],
                team_filter=row["team_filter"],
                season_type=row["season_type"],
                season_id=row["season_id"],
                player_id=row["player_id"],
                player_name=row["player_name"],
                coefficient=row["coefficient"],
                games=row["games"],
                average_minutes=row["average_minutes"],
                total_minutes=row["total_minutes"],
            )
        )


def _load_wowy_metric_rows(
    connection: sqlite3.Connection,
    groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
) -> None:
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
            games_with,
            games_without,
            avg_margin_with,
            avg_margin_without,
            average_minutes,
            total_minutes,
            raw_wowy_score
        FROM wowy_player_season_values
        ORDER BY metric_id, scope_key, season_id, player_id
        """
    ).fetchall()
    for row in rows:
        groups[(row["metric_id"], row["scope_key"])].append(
            WowyPlayerSeasonValueRow(
                metric_id=row["metric_id"],
                scope_key=row["scope_key"],
                team_filter=row["team_filter"],
                season_type=row["season_type"],
                season_id=row["season_id"],
                player_id=row["player_id"],
                player_name=row["player_name"],
                value=row["value"],
                games_with=row["games_with"],
                games_without=row["games_without"],
                avg_margin_with=row["avg_margin_with"],
                avg_margin_without=row["avg_margin_without"],
                average_minutes=row["average_minutes"],
                total_minutes=row["total_minutes"],
                raw_wowy_score=row["raw_wowy_score"],
            )
        )


def _audit_metric_scope_catalog_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> tuple[
    dict[tuple[str, str], MetricScopeCatalogRow],
    dict[tuple[str, str], list[str]],
    dict[tuple[str, str], list[int]],
]:
    rows = connection.execute(
        """
        SELECT
            metric_id,
            scope_key,
            label,
            team_filter,
            season_type,
            full_span_start_season_id,
            full_span_end_season_id,
            updated_at
        FROM metric_scope_catalog
        ORDER BY metric_id, scope_key
        """
    ).fetchall()
    season_rows = connection.execute(
        """
        SELECT
            metric_id,
            scope_key,
            season_id
        FROM metric_scope_season
        ORDER BY metric_id, scope_key, season_id
        """
    ).fetchall()
    team_rows = connection.execute(
        """
        SELECT
            metric_id,
            scope_key,
            team_id
        FROM metric_scope_team
        ORDER BY metric_id, scope_key, team_id
        """
    ).fetchall()
    scope_season_rows: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in season_rows:
        scope_season_rows[(row["metric_id"], row["scope_key"])].append(row["season_id"])
    scope_team_rows: dict[tuple[str, str], list[int]] = defaultdict(list)
    for row in team_rows:
        scope_team_rows[(row["metric_id"], row["scope_key"])].append(row["team_id"])
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow] = {}
    for row in rows:
        key = (row["metric_id"], row["scope_key"])
        catalog_row = MetricScopeCatalogRow(
            metric_id=row["metric_id"],
            scope_key=row["scope_key"],
            label=row["label"],
            team_filter=row["team_filter"],
            season_type=row["season_type"],
            available_season_ids=list(scope_season_rows.get(key, [])),
            available_team_ids=list(scope_team_rows.get(key, [])),
            full_span_start_season_id=row["full_span_start_season_id"],
            full_span_end_season_id=row["full_span_end_season_id"],
            updated_at=row["updated_at"],
        )
        catalog_rows[key] = catalog_row
        try:
            validate_metric_scope_catalog_row(catalog_row)
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    "metric_scope_catalog",
                    f"metric={catalog_row.metric_id!r},scope_key={catalog_row.scope_key!r}",
                    str(exc),
                )
            )
    return catalog_rows, dict(scope_season_rows), dict(scope_team_rows)


def _audit_metric_full_span_tables(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
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
            validate_metric_full_span_rows(
                metric_id=key[0],
                scope_key=key[1],
                series_rows=series,
                point_rows=points,
            )
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    "metric_full_span",
                    f"metric={key[0]!r},scope_key={key[1]!r}",
                    str(exc),
                )
            )

    return groups


def _load_json_list(value: str) -> list[str]:
    import json

    loaded = json.loads(value)
    assert isinstance(loaded, list), "metric store JSON column must decode to a list"
    return loaded
