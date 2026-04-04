from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    WowyPlayerValue,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy.defaults import describe_metric
from rawr_analytics.metrics.wowy.inputs import WowyRequest, WowySeasonInput
from rawr_analytics.metrics.wowy.records import WowyPlayerSeasonRecord
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary


@dataclass(frozen=True)
class WowyPlayerSeasonValue:
    season_id: str
    player: PlayerSummary
    minutes: PlayerMinutes
    result: WowyPlayerValue


@dataclass(frozen=True)
class WowyCustomQueryResult:
    metric: str
    metric_label: str
    rows: list[WowyPlayerSeasonValue]


def prepare_wowy_player_season_records(
    *,
    season_inputs: list[WowySeasonInput],
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[WowyPlayerSeasonRecord]:
    return WowyPlayerSeasonRecord.build_player_season_records(
        WowyRequest(
            season_inputs=season_inputs,
            min_games_with=min_games_with,
            min_games_without=min_games_without,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
    )


def build_wowy_custom_query(
    metric: Metric,
    *,
    season_inputs: list[WowySeasonInput],
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> WowyCustomQueryResult:
    records = prepare_wowy_player_season_records(
        season_inputs=season_inputs,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return WowyCustomQueryResult(
        metric=metric.value,
        metric_label=describe_metric(metric).label,
        rows=[_build_wowy_query_row(metric, record) for record in records],
    )


def _build_wowy_query_row(
    metric: Metric,
    record: WowyPlayerSeasonRecord,
) -> WowyPlayerSeasonValue:
    if metric == Metric.WOWY:
        return WowyPlayerSeasonValue(
            season_id=record.season.id,
            player=record.player,
            minutes=record.minutes,
            result=WowyPlayerValue(
                games_with=record.result.games_with,
                games_without=record.result.games_without,
                avg_margin_with=record.result.avg_margin_with,
                avg_margin_without=record.result.avg_margin_without,
                value=record.result.value,
                raw_value=None,
            ),
        )
    if metric == Metric.WOWY_SHRUNK:
        return WowyPlayerSeasonValue(
            season_id=record.season.id,
            player=record.player,
            minutes=record.minutes,
            result=WowyPlayerValue(
                games_with=record.result.games_with,
                games_without=record.result.games_without,
                avg_margin_with=record.result.avg_margin_with,
                avg_margin_without=record.result.avg_margin_without,
                value=compute_wowy_shrinkage_score(
                    games_with=record.result.games_with,
                    games_without=record.result.games_without,
                    wowy_score=record.result.value,
                    prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
                ),
                raw_value=record.result.raw_value,
            ),
        )
    raise ValueError(f"Unknown WOWY metric: {metric}")
