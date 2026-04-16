from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import cast

from rawr_analytics.data._validation_issue import ValidationIssue
from rawr_analytics.data.metric_store._catalog import MetricScopeCatalogRow, catalog_seasons
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
    build_rawr_player_season_value_row,
    build_wowy_player_season_value_row,
)
from rawr_analytics.data.metric_store._validation import (
    validate_metric_scope_catalog_row,
    validate_rawr_rows,
    validate_wowy_rows,
)


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


def audit_metric_store_tables(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> MetricStoreAuditState:
    catalog_rows, scope_season_rows, scope_team_rows = _audit_metric_scope_catalog_table(
        connection,
        issues,
    )
    rawr_row_groups, wowy_row_groups, metadata_rows = _audit_metric_player_season_values_table(
        connection,
        issues,
        catalog_rows,
    )
    return MetricStoreAuditState(
        rawr_row_groups=rawr_row_groups,
        wowy_row_groups=wowy_row_groups,
        metadata_rows=metadata_rows,
        catalog_rows=catalog_rows,
        scope_season_rows=scope_season_rows,
        scope_team_rows=scope_team_rows,
    )


def _audit_metric_player_season_values_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow],
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
        (cast(str, row["metric_id"]), cast(str, row["scope_key"])): MetricStoreAuditMetadata(
            source_table="metric_snapshot",
            snapshot_id=cast(int | None, row["snapshot_id"]),
            build_version=cast(str, row["build_version"]),
            source_fingerprint=cast(str, row["source_fingerprint"]),
            row_count=cast(int, row["row_count"]),
        )
        for row in snapshot_rows
    }

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
        catalog_row = catalog_rows.get(key)
        if catalog_row is None:
            issues.append(
                ValidationIssue(
                    "rawr_player_season_values",
                    f"metric={key[0]!r},scope_key={key[1]!r}",
                    "Metric rows are missing a matching metric_scope_catalog row",
                )
            )
            continue
        try:
            validate_rawr_rows(
                scope_key=key[1],
                team_filter=catalog_row.team_filter,
                seasons=catalog_seasons(catalog_row),
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
                source_table="metric_snapshot",
                snapshot_id=None,
                build_version="missing-metadata",
                source_fingerprint="missing-metadata",
                row_count=-1,
            ),
        )
        catalog_row = catalog_rows.get(key)
        if catalog_row is None:
            issues.append(
                ValidationIssue(
                    "wowy_player_season_values",
                    f"metric={key[0]!r},scope_key={key[1]!r}",
                    "Metric rows are missing a matching metric_scope_catalog row",
                )
            )
            continue
        try:
            validate_wowy_rows(
                metric_id=key[0],
                scope_key=key[1],
                team_filter=catalog_row.team_filter,
                seasons=catalog_seasons(catalog_row),
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
            snapshot.metric_id,
            snapshot.scope_key,
            rawr.season_id,
            rawr.player_id,
            rawr.player_name,
            rawr.coefficient,
            rawr.games,
            rawr.average_minutes,
            rawr.total_minutes
        FROM rawr_player_season_values AS rawr
        INNER JOIN metric_snapshot AS snapshot
            ON snapshot.snapshot_id = rawr.snapshot_id
        ORDER BY metric_id, scope_key, rawr.season_id, rawr.player_id
        """
    ).fetchall()
    for row in rows:
        groups[_metric_scope_key(row)].append(build_rawr_player_season_value_row(row))


def _load_wowy_metric_rows(
    connection: sqlite3.Connection,
    groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
) -> None:
    rows = connection.execute(
        """
        SELECT
            snapshot.metric_id,
            snapshot.scope_key,
            wowy.season_id,
            wowy.player_id,
            wowy.player_name,
            wowy.value,
            wowy.games_with,
            wowy.games_without,
            wowy.avg_margin_with,
            wowy.avg_margin_without,
            wowy.average_minutes,
            wowy.total_minutes,
            wowy.raw_wowy_score
        FROM wowy_player_season_values AS wowy
        INNER JOIN metric_snapshot AS snapshot
            ON snapshot.snapshot_id = wowy.snapshot_id
        ORDER BY metric_id, scope_key, wowy.season_id, wowy.player_id
        """
    ).fetchall()
    for row in rows:
        groups[_metric_scope_key(row)].append(build_wowy_player_season_value_row(row))


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
        scope_season_rows[_metric_scope_key(row)].append(cast(str, row["season_id"]))
    scope_team_rows: dict[tuple[str, str], list[int]] = defaultdict(list)
    for row in team_rows:
        scope_team_rows[_metric_scope_key(row)].append(cast(int, row["team_id"]))
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow] = {}
    for row in rows:
        key = _metric_scope_key(row)
        catalog_row = MetricScopeCatalogRow(
            metric_id=cast(str, row["metric_id"]),
            scope_key=cast(str, row["scope_key"]),
            label=cast(str, row["label"]),
            team_filter=cast(str, row["team_filter"]),
            season_type=cast(str, row["season_type"]),
            available_season_ids=list(scope_season_rows.get(key, [])),
            available_team_ids=list(scope_team_rows.get(key, [])),
            full_span_start_season_id=cast(str | None, row["full_span_start_season_id"]),
            full_span_end_season_id=cast(str | None, row["full_span_end_season_id"]),
            updated_at=cast(str, row["updated_at"]),
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
def _metric_scope_key(row: sqlite3.Row) -> tuple[str, str]:
    return (cast(str, row["metric_id"]), cast(str, row["scope_key"]))
