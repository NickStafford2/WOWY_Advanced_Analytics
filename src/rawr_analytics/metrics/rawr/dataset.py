from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    describe_rawr_metric,
)
from rawr_analytics.metrics.rawr.inputs import RawrRequest, RawrSeasonInput
from rawr_analytics.metrics.rawr.records import RawrPlayerSeasonRecord, build_player_season_records


@dataclass(frozen=True)
class RawrCustomQueryResult:
    metric: str
    metric_label: str
    rows: list[RawrPlayerSeasonRecord]


def build_rawr_custom_query_result(
    *,
    season_inputs: list[RawrSeasonInput],
    min_games: int,
    ridge_alpha: float,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> RawrCustomQueryResult:
    return RawrCustomQueryResult(
        metric=Metric.RAWR.value,
        metric_label=describe_rawr_metric().label,
        rows=build_player_season_records(
            RawrRequest(
            season_inputs=season_inputs,
            min_games=min_games,
            ridge_alpha=ridge_alpha,
            shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
            shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
            shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
            )
        ),
    )
