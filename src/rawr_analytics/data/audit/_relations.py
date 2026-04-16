from __future__ import annotations

from rawr_analytics.data._validation import ValidationIssue
from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.data.metric_store._tables import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
)
from rawr_analytics.data.metric_store.audit import MetricStoreAuditMetadata
from rawr_analytics.metrics._metric_cache_key import MetricCacheKey
from rawr_analytics.shared.season import Season


def validate_metric_store_relations(
    *,
    rawr_row_groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]],
    wowy_row_groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
    metadata_rows: dict[tuple[str, str], MetricStoreAuditMetadata],
    cache_keys: dict[tuple[str, str], MetricCacheKey],
    issues: list[ValidationIssue],
) -> None:
    metric_row_groups = rawr_row_groups | wowy_row_groups
    metric_cache_keys = set(metric_row_groups)
    metadata_cache_keys = set(metadata_rows)
    parsed_cache_keys = set(cache_keys)

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

        cache_key = cache_keys.get(key)
        if cache_key is None:
            issues.append(
                ValidationIssue(
                    table="metric_cache_entry",
                    key=f"metric={metric!r},metric_cache_key={metric_cache_key!r}",
                    message="missing parseable metric cache key",
                )
            )
        elif not season_set.issubset(set(cache_key.season_ids)):
            issues.append(
                ValidationIssue(
                    table="metric_cache_entry",
                    key=f"metric={metric!r},metric_cache_key={metric_cache_key!r}",
                    message=(
                        "season_ids is missing seasons present in metric value rows: "
                        f"cache_key={cache_key.season_ids!r} "
                        f"metric_value_rows={seasons!r}"
                    ),
                )
            )

    all_cache_keys = metric_cache_keys | metadata_cache_keys | parsed_cache_keys
    for key in sorted(all_cache_keys):
        metric, metric_cache_key = key
        metadata_row = metadata_rows.get(key)
        cache_key = cache_keys.get(key)

        if metadata_row is None:
            continue

        seasons = cache_key.season_ids if cache_key is not None else []
        if not seasons:
            continue

        current_fingerprint = _load_current_fingerprint(seasons)
        if current_fingerprint is None:
            continue

        if metadata_row.source_fingerprint != current_fingerprint:
            issues.append(
                ValidationIssue(
                    table=metadata_row.source_table,
                    key=f"metric={metric!r},metric_cache_key={metric_cache_key!r}",
                    message="source_fingerprint does not match normalized cache for cache-key seasons",
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

    for key in sorted(parsed_cache_keys - metric_cache_keys):
        issues.append(
            ValidationIssue(
                table="metric_cache_entry",
                key=f"metric={key[0]!r},metric_cache_key={key[1]!r}",
                message="cache key has no matching metric value rows",
            )
        )


def _load_current_fingerprint(season_ids: list[str]) -> str | None:
    seasons = [Season.parse_id(season_id) for season_id in season_ids]
    snapshot = load_game_cache_snapshot(seasons=seasons)
    if not snapshot.entries:
        return None
    return snapshot.fingerprint
