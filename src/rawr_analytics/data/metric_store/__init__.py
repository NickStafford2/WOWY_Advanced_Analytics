"""Public repository interface for metric store storage."""

from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.metric_store._queries import (
    MetricSnapshotState,
    load_metric_full_span_points_map,
    load_metric_full_span_series_rows,
    load_metric_scope_catalog_row,
    load_metric_snapshot_state,
)
from rawr_analytics.data.metric_store.audit import (
    MetricStoreAuditMetadata,
    audit_metric_store_tables,
)
from rawr_analytics.data.metric_store.full_span import (
    MetricFullSpanPointRow,
    MetricFullSpanSeries,
    MetricFullSpanSeriesRow,
)
from rawr_analytics.data.metric_store.rawr import (
    RawrPlayerSeasonValueRow,
    load_rawr_player_season_value_rows,
)
from rawr_analytics.data.metric_store.schema import initialize_player_metrics_db
from rawr_analytics.data.metric_store.store import (
    MetricScopeAvailability,
    MetricScopeCatalog,
    MetricScopeCatalogRow,
    MetricSeasonSpanIds,
    clear_metric_scope_store,
    replace_rawr_scope_snapshot,
    replace_wowy_scope_snapshot,
)
from rawr_analytics.data.metric_store.wowy import (
    WowyPlayerSeasonValueRow,
    load_wowy_player_season_value_rows,
)
from rawr_analytics.shared.player import PlayerSummary


@dataclass(frozen=True)
class MetricScopeStoreState:
    catalog_row: MetricScopeCatalogRow
    snapshot_state: MetricSnapshotState


@dataclass(frozen=True)
class MetricSpanStoreRows:
    series: list[MetricFullSpanSeries]


def load_metric_scope_store_state(
    metric: str,
    scope_key: str,
) -> MetricScopeStoreState | None:
    catalog_row = load_metric_scope_catalog_row(metric, scope_key)
    if catalog_row is None:
        return None
    snapshot_state = load_metric_snapshot_state(metric, scope_key)
    if snapshot_state is None:
        return None
    return MetricScopeStoreState(catalog_row=catalog_row, snapshot_state=snapshot_state)


def load_metric_span_store_rows(
    *,
    metric: str,
    scope_key: str,
    top_n: int | None = None,
) -> MetricSpanStoreRows:
    series_rows = load_metric_full_span_series_rows(
        metric=metric,
        scope_key=scope_key,
        top_n=top_n,
    )
    points_map = load_metric_full_span_points_map(
        metric=metric,
        scope_key=scope_key,
        player_ids=[row.player_id for row in series_rows],
    )
    return MetricSpanStoreRows(
        series=[
            MetricFullSpanSeries(
                player=PlayerSummary(
                    player_id=row.player_id,
                    player_name=row.player_name,
                ),
                span_average_value=row.span_average_value,
                season_count=row.season_count,
                rank_order=row.rank_order,
                points_by_season=dict(points_map.get(row.player_id, {})),
            )
            for row in series_rows
        ],
    )


__all__ = [
    "MetricFullSpanPointRow",
    "MetricFullSpanSeries",
    "MetricFullSpanSeriesRow",
    "MetricScopeAvailability",
    "MetricScopeCatalog",
    "MetricScopeCatalogRow",
    "MetricScopeStoreState",
    "MetricSeasonSpanIds",
    "MetricSnapshotState",
    "MetricSpanStoreRows",
    "MetricStoreAuditMetadata",
    "RawrPlayerSeasonValueRow",
    "WowyPlayerSeasonValueRow",
    "audit_metric_store_tables",
    "clear_metric_scope_store",
    "initialize_player_metrics_db",
    "load_metric_scope_store_state",
    "load_metric_span_store_rows",
    "load_rawr_player_season_value_rows",
    "load_wowy_player_season_value_rows",
    "replace_rawr_scope_snapshot",
    "replace_wowy_scope_snapshot",
]
