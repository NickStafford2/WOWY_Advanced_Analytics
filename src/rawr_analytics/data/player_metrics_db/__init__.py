"""Public repository interface for player metric storage."""

from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.player_metrics_db.models import (
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    MetricStoreMetadata,
)
from rawr_analytics.data.player_metrics_db.queries import (
    load_metric_full_span_points_map,
    load_metric_full_span_series_rows,
    load_metric_scope_catalog_row,
    load_metric_store_metadata,
)
from rawr_analytics.data.player_metrics_db.rawr import (
    RawrPlayerSeasonValueRow,
    load_rawr_player_season_value_rows,
)
from rawr_analytics.data.player_metrics_db.schema import initialize_player_metrics_db
from rawr_analytics.data.player_metrics_db.store import (
    clear_metric_scope_store,
    replace_rawr_scope_snapshot,
    replace_wowy_scope_snapshot,
)
from rawr_analytics.data.player_metrics_db.wowy import (
    WowyPlayerSeasonValueRow,
    load_wowy_player_season_value_rows,
)


@dataclass(frozen=True)
class MetricScopeStoreState:
    catalog_row: MetricScopeCatalogRow
    metadata: MetricStoreMetadata


@dataclass(frozen=True)
class MetricSpanStoreRows:
    series_rows: list[MetricFullSpanSeriesRow]
    points_map: dict[int, dict[str, float]]


def load_metric_scope_store_state(
    metric: str,
    scope_key: str,
) -> MetricScopeStoreState | None:
    catalog_row = load_metric_scope_catalog_row(metric, scope_key)
    if catalog_row is None:
        return None
    metadata = load_metric_store_metadata(metric, scope_key)
    if metadata is None:
        return None
    return MetricScopeStoreState(catalog_row=catalog_row, metadata=metadata)


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
    return MetricSpanStoreRows(
        series_rows=series_rows,
        points_map=load_metric_full_span_points_map(
            metric=metric,
            scope_key=scope_key,
            player_ids=[row.player_id for row in series_rows],
        ),
    )


__all__ = [
    "MetricScopeCatalogRow",
    "MetricScopeStoreState",
    "MetricSpanStoreRows",
    "RawrPlayerSeasonValueRow",
    "WowyPlayerSeasonValueRow",
    "clear_metric_scope_store",
    "initialize_player_metrics_db",
    "load_metric_scope_store_state",
    "load_metric_span_store_rows",
    "load_rawr_player_season_value_rows",
    "load_wowy_player_season_value_rows",
    "replace_rawr_scope_snapshot",
    "replace_wowy_scope_snapshot",
]
