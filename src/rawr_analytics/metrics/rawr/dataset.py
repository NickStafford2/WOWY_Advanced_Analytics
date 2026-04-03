from __future__ import annotations

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    describe_metric,
)
from rawr_analytics.metrics.rawr.models import (
    RawrCustomQueryResult,
    RawrPlayerSeasonRecord,
    RawrPlayerSeasonValue,
    RawrRequest,
    RawrSeasonInput,
)
from rawr_analytics.metrics.rawr.records import build_player_season_records


def prepare_rawr_player_season_records(
    *,
    season_inputs: list[RawrSeasonInput],
    min_games: int,
    ridge_alpha: float,
    shrinkage_mode: str,
    shrinkage_strength: float,
    shrinkage_minute_scale: float,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[RawrPlayerSeasonRecord]:
    return build_player_season_records(
        RawrRequest(
            season_inputs=season_inputs,
            min_games=min_games,
            ridge_alpha=ridge_alpha,
            shrinkage_mode=shrinkage_mode,
            shrinkage_strength=shrinkage_strength,
            shrinkage_minute_scale=shrinkage_minute_scale,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
    )


def build_rawr_custom_query(
    *,
    season_inputs: list[RawrSeasonInput],
    min_games: int,
    ridge_alpha: float,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> RawrCustomQueryResult:
    records = prepare_rawr_player_season_records(
        season_inputs=season_inputs,
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return RawrCustomQueryResult(
        metric=Metric.RAWR.value,
        metric_label=describe_metric().label,
        rows=[
            RawrPlayerSeasonValue(
                season_id=record.season.id,
                player_id=record.player_id,
                player_name=record.player_name,
                coefficient=record.coefficient,
                games=record.games,
                average_minutes=record.average_minutes,
                total_minutes=record.total_minutes,
            )
            for record in records
        ],
    )
