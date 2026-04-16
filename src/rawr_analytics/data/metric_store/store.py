from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.metric_store._catalog import MetricCacheCatalogRow
from rawr_analytics.data.metric_store._queries import (
    MetricSnapshotState,
    load_metric_cache_catalog_row,
    load_metric_cache_entry_state,
)
from rawr_analytics.data.metric_store.full_span import (
    MetricFullSpanSeries,
    MetricStorePlayerSeasonValue,
    build_metric_full_span_series,
)
from rawr_analytics.data.metric_store.rawr import load_rawr_player_season_value_rows
from rawr_analytics.data.metric_store.wowy import load_wowy_player_season_value_rows


@dataclass(frozen=True)
class MetricScopeStoreState:
    catalog_row: MetricCacheCatalogRow
    snapshot_state: MetricSnapshotState


@dataclass(frozen=True)
class MetricSpanStoreRows:
    series: list[MetricFullSpanSeries]


def load_metric_cache_store_state(
    metric: str,
    metric_cache_key: str,
) -> MetricScopeStoreState | None:
    catalog_row = load_metric_cache_catalog_row(metric, metric_cache_key)
    if catalog_row is None:
        return None
    snapshot_state = load_metric_cache_entry_state(metric, metric_cache_key)
    if snapshot_state is None:
        return None
    return MetricScopeStoreState(catalog_row=catalog_row, snapshot_state=snapshot_state)


def load_metric_cache_span_rows(
    *,
    metric: str,
    metric_cache_key: str,
    top_n: int | None = None,
) -> MetricSpanStoreRows:
    catalog_row = load_metric_cache_catalog_row(metric, metric_cache_key)
    assert catalog_row is not None, "metric span reads require an existing catalog row"
    player_season_values = _load_metric_player_season_values(
        metric=metric,
        metric_cache_key=metric_cache_key,
        season_ids=catalog_row.available_season_ids,
    )
    return MetricSpanStoreRows(
        series=build_metric_full_span_series(
            season_ids=catalog_row.available_season_ids,
            player_season_values=player_season_values,
            top_n=top_n,
        )
    )


def _load_metric_player_season_values(
    *,
    metric: str,
    metric_cache_key: str,
    season_ids: list[str],
) -> list[MetricStorePlayerSeasonValue]:
    if metric == "rawr":
        return [
            MetricStorePlayerSeasonValue(
                player_id=row.player_id,
                player_name=row.player_name,
                season_id=row.season_id,
                value=row.coefficient,
            )
            for row in load_rawr_player_season_value_rows(
                metric_cache_key=metric_cache_key,
                seasons=season_ids,
            )
        ]
    if metric in {"wowy", "wowy_shrunk"}:
        return [
            MetricStorePlayerSeasonValue(
                player_id=row.player_id,
                player_name=row.player_name,
                season_id=row.season_id,
                value=row.value,
            )
            for row in load_wowy_player_season_value_rows(
                metric_id=metric,
                metric_cache_key=metric_cache_key,
                seasons=season_ids,
            )
            if row.value is not None
        ]
    raise ValueError(f"Unsupported metric span read for {metric!r}")


__all__ = [
    "MetricScopeStoreState",
    "MetricSpanStoreRows",
    "load_metric_cache_span_rows",
    "load_metric_cache_store_state",
]
