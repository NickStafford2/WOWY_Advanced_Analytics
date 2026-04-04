from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr.analysis import RawrValue
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    describe_metric,
)
from rawr_analytics.metrics.rawr.inputs import RawrRequest, RawrSeasonInput
from rawr_analytics.metrics.rawr.records import RawrPlayerSeasonRecord
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary


@dataclass(frozen=True)
class RawrPlayerSeasonValue:
    season_id: str
    player: PlayerSummary
    minutes: PlayerMinutes
    result: RawrValue


@dataclass(frozen=True)
class RawrCustomQueryResult:
    metric: str
    metric_label: str
    rows: list[RawrPlayerSeasonValue]

    @staticmethod
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
                    player=record.player,
                    minutes=record.minutes,
                    result=record.result,
                )
                for record in records
            ],
        )


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
    return RawrPlayerSeasonRecord.build_player_season_records(
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
