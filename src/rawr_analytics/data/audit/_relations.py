from __future__ import annotations

from rawr_analytics.data._validation_issue import ValidationIssue
from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.data.metric_store._catalog import MetricCacheCatalogRow
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
)
from rawr_analytics.data.metric_store.audit import MetricStoreAuditMetadata
from rawr_analytics.shared.season import Season, SeasonType


def validate_metric_store_relations(
    *,
    rawr_row_groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]],
    wowy_row_groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
    metadata_rows: dict[tuple[str, str], MetricStoreAuditMetadata],
    catalog_rows: dict[tuple[str, str], MetricCacheCatalogRow],
    catalog_season_rows: dict[tuple[str, str], list[str]],
    issues: list[ValidationIssue],
) -> None:
    metric_row_groups = rawr_row_groups | wowy_row_groups
    metric_cache_keys = set(metric_row_groups)
    metadata_cache_keys = set(metadata_rows)
    catalog_cache_keys = set(catalog_rows)
    catalog_season_cache_keys = set(catalog_season_rows)
    _, fingerprint_by_season_type = _load_normalized_cache_state()

    for key, rows in metric_row_groups.items():
        metric, metric_cache_key = key
        seasons = sorted({row.season_id for row in rows})
        season_set = set(seasons)

        metadata_row = metadata_rows.get(key)
        metadata_table = (
            metadata_row.source_table if metadata_row is not None else "metric_cache_entry"
        )
        if metadata_row is None:
            issues.append(
                ValidationIssue(
                    table=metadata_table,
                    key=f"metric={metric!r},metric_cache_key={metric_cache_key!r}",
                    message="missing cache entry row for metric cache key",
                )
            )
        elif metadata_row.row_count != len(rows):
            issues.append(
                ValidationIssue(
                    table=metadata_table,
                    key=f"metric={metric!r},metric_cache_key={metric_cache_key!r}",
                    message=(
                        "row_count does not match metric value rows: "
                        f"{metadata_row.row_count} != {len(rows)}"
                    ),
                )
            )

        catalog_row = catalog_rows.get(key)
        if catalog_row is None:
            issues.append(
                ValidationIssue(
                    table="metric_cache_catalog",
                    key=f"metric={metric!r},metric_cache_key={metric_cache_key!r}",
                    message="missing catalog row for metric cache key",
                )
            )
        elif not season_set.issubset(set(catalog_row.season_ids)):
            issues.append(
                ValidationIssue(
                    table="metric_cache_catalog",
                    key=f"metric={metric!r},metric_cache_key={metric_cache_key!r}",
                    message=(
                        "season_ids is missing seasons present in metric value rows: "
                        f"catalog={catalog_row.season_ids!r} "
                        f"metric_value_rows={seasons!r}"
                    ),
                )
            )

        if catalog_row is not None and catalog_season_rows.get(key, []) != catalog_row.season_ids:
            issues.append(
                ValidationIssue(
                    table="metric_cache_season",
                    key=f"metric={metric!r},metric_cache_key={metric_cache_key!r}",
                    message="catalog season rows do not match catalog season_ids",
                )
            )

    all_cache_keys = metric_cache_keys | metadata_cache_keys | catalog_cache_keys
    for key in sorted(all_cache_keys):
        metric, metric_cache_key = key
        metadata_row = metadata_rows.get(key)
        catalog_row = catalog_rows.get(key)

        if metadata_row is None:
            continue

        seasons = catalog_row.season_ids if catalog_row is not None else []
        if not seasons:
            continue

        season_types = {Season.parse_id(season_id).season_type.value for season_id in seasons}
        if len(season_types) != 1:
            continue

        season_type = next(iter(season_types))
        current_fingerprint = fingerprint_by_season_type.get(season_type)
        if current_fingerprint is None:
            continue

        if metadata_row.source_fingerprint != current_fingerprint:
            issues.append(
                ValidationIssue(
                    table=metadata_row.source_table,
                    key=f"metric={metric!r},metric_cache_key={metric_cache_key!r}",
                    message=(
                        "source_fingerprint does not match normalized cache for "
                        f"season_type {season_type!r}"
                    ),
                )
            )

    for key in sorted(metadata_cache_keys - metric_cache_keys):
        metadata_table = metadata_rows[key].source_table
        issues.append(
            ValidationIssue(
                table=metadata_table,
                key=f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                message="cache entry row has no matching metric value rows",
            )
        )

    for key in sorted(catalog_cache_keys - metric_cache_keys):
        issues.append(
            ValidationIssue(
                table="metric_cache_catalog",
                key=f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                message="catalog row has no matching metric value rows",
            )
        )

    for key in sorted(catalog_season_cache_keys - catalog_cache_keys):
        issues.append(
            ValidationIssue(
                table="metric_cache_season",
                key=f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                message="catalog season rows have no matching catalog row",
            )
        )


def _load_normalized_cache_state() -> tuple[dict[str, int], dict[str, str]]:
    counts: dict[str, int] = {}
    fingerprints: dict[str, str] = {}

    for season_type in SeasonType:
        snapshot = load_game_cache_snapshot()
        counts[season_type.value] = len(snapshot.entries)
        if snapshot.entries:
            fingerprints[season_type.value] = snapshot.fingerprint

    return counts, fingerprints
