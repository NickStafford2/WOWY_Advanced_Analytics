from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import cast

from rawr_analytics.data._validation_issue import ValidationIssue
from rawr_analytics.data.metric_store._catalog import MetricCacheCatalogRow, catalog_seasons
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
    build_rawr_player_season_value_row,
    build_wowy_player_season_value_row,
)
from rawr_analytics.data.metric_store._validation import (
    validate_metric_cache_catalog_row,
    validate_rawr_rows,
    validate_wowy_rows,
)


@dataclass(frozen=True)
class MetricStoreAuditMetadata:
    source_table: str
    metric_cache_entry_id: int | None
    build_version: str
    source_fingerprint: str
    row_count: int


@dataclass(frozen=True)
class MetricStoreAuditState:
    rawr_row_groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]]
    wowy_row_groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]]
    metadata_rows: dict[tuple[str, str], MetricStoreAuditMetadata]
    catalog_rows: dict[tuple[str, str], MetricCacheCatalogRow]
    catalog_season_rows: dict[tuple[str, str], list[str]]


def audit_metric_store_tables(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> MetricStoreAuditState:
    catalog_rows, catalog_season_rows = _audit_metric_cache_catalog_table(
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
        catalog_season_rows=catalog_season_rows,
    )


def _audit_metric_player_season_values_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
    catalog_rows: dict[tuple[str, str], MetricCacheCatalogRow],
) -> tuple[
    dict[tuple[str, str], list[RawrPlayerSeasonValueRow]],
    dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
    dict[tuple[str, str], MetricStoreAuditMetadata],
]:
    cache_entry_rows = connection.execute(
        """
        SELECT
            metric_cache_entry_id,
            metric_id,
            metric_cache_key,
            build_version,
            source_fingerprint,
            row_count
        FROM metric_cache_entry
        """
    ).fetchall()
    metadata_by_key = {
        (cast(str, row["metric_id"]), cast(str, row["metric_cache_key"])): MetricStoreAuditMetadata(
            source_table="metric_cache_entry",
            metric_cache_entry_id=cast(int | None, row["metric_cache_entry_id"]),
            build_version=cast(str, row["build_version"]),
            source_fingerprint=cast(str, row["source_fingerprint"]),
            row_count=cast(int, row["row_count"]),
        )
        for row in cache_entry_rows
    }

    rawr_groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]] = defaultdict(list)
    wowy_groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]] = defaultdict(list)
    _load_rawr_metric_rows(connection, rawr_groups)
    _load_wowy_metric_rows(connection, wowy_groups)

    for key, group_rows in rawr_groups.items():
        metadata_row = metadata_by_key.get(
            key,
            MetricStoreAuditMetadata(
                source_table="metric_cache_entry",
                metric_cache_entry_id=None,
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
                    f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                    "Metric rows are missing a matching metric_cache_catalog row",
                )
            )
            continue
        try:
            validate_rawr_rows(
                metric_cache_key=key[1],
                seasons=catalog_seasons(catalog_row),
                build_version=metadata_row.build_version,
                source_fingerprint=metadata_row.source_fingerprint,
                rows=group_rows,
            )
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    "rawr_player_season_values",
                    f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                    str(exc),
                )
            )

    for key, group_rows in wowy_groups.items():
        metadata_row = metadata_by_key.get(
            key,
            MetricStoreAuditMetadata(
                source_table="metric_cache_entry",
                metric_cache_entry_id=None,
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
                    f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                    "Metric rows are missing a matching metric_cache_catalog row",
                )
            )
            continue
        try:
            validate_wowy_rows(
                metric_id=key[0],
                metric_cache_key=key[1],
                seasons=catalog_seasons(catalog_row),
                build_version=metadata_row.build_version,
                source_fingerprint=metadata_row.source_fingerprint,
                rows=group_rows,
            )
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    "wowy_player_season_values",
                    f"metric={key[0]!r},metric_cache_key={key[1]!r}",
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
            cache_entry.metric_id,
            cache_entry.metric_cache_key,
            rawr.season_id,
            rawr.player_id,
            rawr.player_name,
            rawr.coefficient,
            rawr.games,
            rawr.average_minutes,
            rawr.total_minutes
        FROM rawr_player_season_values AS rawr
        INNER JOIN metric_cache_entry AS cache_entry
            ON cache_entry.metric_cache_entry_id = rawr.metric_cache_entry_id
        ORDER BY metric_id, metric_cache_key, rawr.season_id, rawr.player_id
        """
    ).fetchall()
    for row in rows:
        groups[_metric_store_key(row)].append(build_rawr_player_season_value_row(row))


def _load_wowy_metric_rows(
    connection: sqlite3.Connection,
    groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
) -> None:
    rows = connection.execute(
        """
        SELECT
            cache_entry.metric_id,
            cache_entry.metric_cache_key,
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
        INNER JOIN metric_cache_entry AS cache_entry
            ON cache_entry.metric_cache_entry_id = wowy.metric_cache_entry_id
        ORDER BY metric_id, metric_cache_key, wowy.season_id, wowy.player_id
        """
    ).fetchall()
    for row in rows:
        groups[_metric_store_key(row)].append(build_wowy_player_season_value_row(row))


def _audit_metric_cache_catalog_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> tuple[
    dict[tuple[str, str], MetricCacheCatalogRow],
    dict[tuple[str, str], list[str]],
]:
    rows = connection.execute(
        """
        SELECT
            metric_id,
            metric_cache_key,
            updated_at
        FROM metric_cache_catalog
        ORDER BY metric_id, metric_cache_key
        """
    ).fetchall()
    season_rows = connection.execute(
        """
        SELECT
            metric_id,
            metric_cache_key,
            season_id
        FROM metric_cache_season
        ORDER BY metric_id, metric_cache_key, season_id
        """
    ).fetchall()
    cache_season_rows: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in season_rows:
        cache_season_rows[_metric_store_key(row)].append(cast(str, row["season_id"]))
    catalog_rows: dict[tuple[str, str], MetricCacheCatalogRow] = {}
    for row in rows:
        key = _metric_store_key(row)
        catalog_row = MetricCacheCatalogRow(
            metric_id=cast(str, row["metric_id"]),
            metric_cache_key=cast(str, row["metric_cache_key"]),
            season_ids=list(cache_season_rows.get(key, [])),
            updated_at=cast(str, row["updated_at"]),
        )
        catalog_rows[key] = catalog_row
        try:
            validate_metric_cache_catalog_row(catalog_row)
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    "metric_cache_catalog",
                    f"metric={catalog_row.metric_id!r},metric_cache_key={catalog_row.metric_cache_key!r}",
                    str(exc),
                )
            )
    return catalog_rows, dict(cache_season_rows)


def _metric_store_key(row: sqlite3.Row) -> tuple[str, str]:
    return (cast(str, row["metric_id"]), cast(str, row["metric_cache_key"]))
