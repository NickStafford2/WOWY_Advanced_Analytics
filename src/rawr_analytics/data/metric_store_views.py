from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rawr_analytics.data.metric_store_query import require_current_metric_scope
from rawr_analytics.data.metric_store import load_metric_span_store_rows
from rawr_analytics.metrics.constants import Metric

__all__ = [
    "CachedMetricSpanSnapshot",
    "load_cached_metric_span_snapshot",
]


@dataclass(frozen=True)
class CachedMetricSpanSnapshot:
    metric: str
    metric_label: str
    start_season: str | None
    end_season: str | None
    available_seasons: list[str]
    top_n: int
    series: list[dict[str, Any]]


def load_cached_metric_span_snapshot(
    *,
    metric: Metric,
    scope_key: str,
    top_n: int,
) -> CachedMetricSpanSnapshot:
    catalog_row = require_current_metric_scope(metric=metric, scope_key=scope_key)
    span_rows = load_metric_span_store_rows(
        metric=metric.value,
        scope_key=scope_key,
        top_n=top_n,
    )
    return CachedMetricSpanSnapshot(
        metric=metric.value,
        metric_label=catalog_row.label,
        start_season=catalog_row.full_span_start_season_id,
        end_season=catalog_row.full_span_end_season_id,
        available_seasons=catalog_row.available_season_ids,
        top_n=top_n,
        series=[
            {
                "player_id": row.player_id,
                "player_name": row.player_name,
                "span_average_value": row.span_average_value,
                "season_count": row.season_count,
                "points": [
                    {
                        "season": season,
                        "value": span_rows.points_map.get(row.player_id, {}).get(season),
                    }
                    for season in catalog_row.available_season_ids
                ],
            }
            for row in span_rows.series_rows
        ],
    )
