from __future__ import annotations

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy.defaults import describe_metric
from rawr_analytics.metrics.wowy.models import (
    WowyCustomQueryResult,
    WowyCustomQueryRow,
    WowyPlayerSeasonRecord,
    WowyRequest,
    WowySeasonInput,
)
from rawr_analytics.metrics.wowy.records import build_player_season_records


def prepare_wowy_player_season_records(
    *,
    season_inputs: list[WowySeasonInput],
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[WowyPlayerSeasonRecord]:
    return build_player_season_records(
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
) -> WowyCustomQueryRow:
    if metric == Metric.WOWY:
        return WowyCustomQueryRow(
            season_id=record.season.id,
            player_id=record.player_id,
            player_name=record.player_name,
            value=record.wowy_score,
            games_with=record.games_with,
            games_without=record.games_without,
            avg_margin_with=record.avg_margin_with,
            avg_margin_without=record.avg_margin_without,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
        )
    if metric == Metric.WOWY_SHRUNK:
        return WowyCustomQueryRow(
            season_id=record.season.id,
            player_id=record.player_id,
            player_name=record.player_name,
            value=compute_wowy_shrinkage_score(
                games_with=record.games_with,
                games_without=record.games_without,
                wowy_score=record.wowy_score,
                prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
            ),
            games_with=record.games_with,
            games_without=record.games_without,
            avg_margin_with=record.avg_margin_with,
            avg_margin_without=record.avg_margin_without,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
            raw_wowy_score=record.wowy_score,
        )
    raise ValueError(f"Unknown WOWY metric: {metric}")
