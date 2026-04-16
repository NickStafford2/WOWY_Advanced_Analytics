from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.metric_store._queries import (
    MetricCacheEntryState,
    list_metric_cache_keys,
    load_metric_cache_entry_state,
)
from rawr_analytics.data.metric_store.full_span import (
    MetricFullSpanSeries,
    MetricStorePlayerSeasonValue,
    build_metric_full_span_series,
)
from rawr_analytics.data.metric_store.rawr import load_rawr_player_season_value_rows
from rawr_analytics.data.metric_store.wowy import load_wowy_player_season_value_rows
from rawr_analytics.metrics._metric_cache_key import MetricCacheKey


@dataclass(frozen=True)
class MetricCacheStoreState:
    cache_key: MetricCacheKey
    cache_entry_state: MetricCacheEntryState


@dataclass(frozen=True)
class MetricSpanStoreRows:
    series: list[MetricFullSpanSeries]


def load_metric_cache_store_state(
    metric: str,
    metric_cache_key: str,
) -> MetricCacheStoreState | None:
    cache_entry_state = load_metric_cache_entry_state(metric, metric_cache_key)
    if cache_entry_state is None:
        return None
    return MetricCacheStoreState(
        cache_key=MetricCacheKey.parse(metric_cache_key),
        cache_entry_state=cache_entry_state,
    )


def load_metric_cache_span_rows(
    *,
    metric: str,
    metric_cache_key: str,
    top_n: int | None = None,
) -> MetricSpanStoreRows:
    cache_key = MetricCacheKey.parse(metric_cache_key)
    player_season_values = _load_metric_player_season_values(
        metric=metric,
        metric_cache_key=metric_cache_key,
        season_ids=cache_key.season_ids,
    )
    return MetricSpanStoreRows(
        series=build_metric_full_span_series(
            season_ids=cache_key.season_ids,
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
    "MetricCacheStoreState",
    "MetricSpanStoreRows",
    "list_metric_cache_keys",
    "load_metric_cache_span_rows",
    "load_metric_cache_store_state",
]
