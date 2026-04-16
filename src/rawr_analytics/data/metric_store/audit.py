from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import cast

from rawr_analytics.data._validation import ValidationIssue
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
    build_rawr_player_season_value_row,
    build_wowy_player_season_value_row,
)
from rawr_analytics.data.metric_store._validation import validate_rawr_rows, validate_wowy_rows
from rawr_analytics.metrics._metric_cache_key import MetricCacheKey
from rawr_analytics.shared.season import Season


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
    cache_keys: dict[tuple[str, str], MetricCacheKey]


def audit_metric_store_tables(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> MetricStoreAuditState:
    cache_keys, metadata_rows = _load_cache_metadata(connection, issues)
    rawr_row_groups, wowy_row_groups = _load_metric_player_season_values(connection)
    _validate_metric_player_season_values(
        issues=issues,
        rawr_row_groups=rawr_row_groups,
        wowy_row_groups=wowy_row_groups,
        metadata_rows=metadata_rows,
        cache_keys=cache_keys,
    )
    return MetricStoreAuditState(
        rawr_row_groups=rawr_row_groups,
        wowy_row_groups=wowy_row_groups,
        metadata_rows=metadata_rows,
        cache_keys=cache_keys,
    )


def _load_cache_metadata(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> tuple[
    dict[tuple[str, str], MetricCacheKey],
    dict[tuple[str, str], MetricStoreAuditMetadata],
]:
    rows = connection.execute(
        """
        SELECT
            metric_cache_entry_id,
            metric_id,
            metric_cache_key,
            build_version,
            source_fingerprint,
            row_count
        FROM metric_cache_entry
        ORDER BY metric_id, metric_cache_key
        """
    ).fetchall()
    cache_keys: dict[tuple[str, str], MetricCacheKey] = {}
    metadata_rows: dict[tuple[str, str], MetricStoreAuditMetadata] = {}
    for row in rows:
        key = _metric_store_key(row)
        metadata_rows[key] = MetricStoreAuditMetadata(
            source_table="metric_cache_entry",
            metric_cache_entry_id=cast(int | None, row["metric_cache_entry_id"]),
            build_version=cast(str, row["build_version"]),
            source_fingerprint=cast(str, row["source_fingerprint"]),
            row_count=cast(int, row["row_count"]),
        )
        try:
            cache_keys[key] = MetricCacheKey.parse(cast(str, row["metric_cache_key"]))
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    "metric_cache_entry",
                    f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                    str(exc),
                )
            )
    return cache_keys, metadata_rows


def _load_metric_player_season_values(
    connection: sqlite3.Connection,
) -> tuple[
    dict[tuple[str, str], list[RawrPlayerSeasonValueRow]],
    dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
]:
    rawr_groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]] = defaultdict(list)
    wowy_groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]] = defaultdict(list)
    rawr_rows = connection.execute(
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
    for row in rawr_rows:
        rawr_groups[_metric_store_key(row)].append(build_rawr_player_season_value_row(row))
    wowy_rows = connection.execute(
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
    for row in wowy_rows:
        wowy_groups[_metric_store_key(row)].append(build_wowy_player_season_value_row(row))
    return rawr_groups, wowy_groups


def _validate_metric_player_season_values(
    *,
    issues: list[ValidationIssue],
    rawr_row_groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]],
    wowy_row_groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
    metadata_rows: dict[tuple[str, str], MetricStoreAuditMetadata],
    cache_keys: dict[tuple[str, str], MetricCacheKey],
) -> None:
    for key, group_rows in rawr_row_groups.items():
        metadata_row = metadata_rows.get(
            key,
            MetricStoreAuditMetadata(
                source_table="metric_cache_entry",
                metric_cache_entry_id=None,
                build_version="missing-metadata",
                source_fingerprint="missing-metadata",
                row_count=-1,
            ),
        )
        cache_key = cache_keys.get(key)
        if cache_key is None:
            issues.append(
                ValidationIssue(
                    "rawr_player_season_values",
                    f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                    "Metric rows are missing a parseable metric cache key",
                )
            )
            continue
        try:
            validate_rawr_rows(
                metric_cache_key=key[1],
                seasons=_parse_seasons(cache_key.season_ids),
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
    for key, group_rows in wowy_row_groups.items():
        metadata_row = metadata_rows.get(
            key,
            MetricStoreAuditMetadata(
                source_table="metric_cache_entry",
                metric_cache_entry_id=None,
                build_version="missing-metadata",
                source_fingerprint="missing-metadata",
                row_count=-1,
            ),
        )
        cache_key = cache_keys.get(key)
        if cache_key is None:
            issues.append(
                ValidationIssue(
                    "wowy_player_season_values",
                    f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                    "Metric rows are missing a parseable metric cache key",
                )
            )
            continue
        try:
            validate_wowy_rows(
                metric_id=key[0],
                metric_cache_key=key[1],
                seasons=_parse_seasons(cache_key.season_ids),
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


def _metric_store_key(row: sqlite3.Row) -> tuple[str, str]:
    return (cast(str, row["metric_id"]), cast(str, row["metric_cache_key"]))


def _parse_seasons(season_ids: list[str]) -> list[Season]:
    return [Season.parse_id(season_id) for season_id in season_ids]
