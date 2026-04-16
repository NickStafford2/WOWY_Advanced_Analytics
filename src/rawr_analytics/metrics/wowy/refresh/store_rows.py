from __future__ import annotations

from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy._calc_vars import WowyCalcVars
from rawr_analytics.metrics.wowy.calculate.records import WowyPlayerSeasonRecord
from rawr_analytics.metrics.wowy.calculate.shrinkage import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)
from rawr_analytics.metrics.wowy.refresh.records import build_wowy_refresh_records


def build_wowy_metric_store_rows(
    *,
    metric: Metric,
    calc_vars: WowyCalcVars,
) -> list[WowyPlayerSeasonValueRow]:
    records = build_wowy_refresh_records(
        calc_vars=calc_vars,
    )
    return [
        _build_wowy_store_row_from_record(
            metric=metric,
            calc_vars=calc_vars,
            record=record,
        )
        for record in records
    ]


def _build_wowy_store_row_from_record(
    *,
    metric: Metric,
    calc_vars: WowyCalcVars,
    record: WowyPlayerSeasonRecord,
) -> WowyPlayerSeasonValueRow:
    value = record.result.value
    include_raw_wowy_score = False
    if metric == Metric.WOWY_SHRUNK:
        include_raw_wowy_score = True
        value = compute_wowy_shrinkage_score(
            games_with=record.result.games_with,
            games_without=record.result.games_without,
            wowy_score=record.result.value,
            prior_games=(
                calc_vars.shrinkage_prior_games
                if calc_vars.shrinkage_prior_games is not None
                else DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES
            ),
        )
    return WowyPlayerSeasonValueRow(
        season_id=record.season.id,
        player_id=record.player.player_id,
        player_name=record.player.player_name,
        value=value,
        games_with=record.result.games_with,
        games_without=record.result.games_without,
        avg_margin_with=record.result.avg_margin_with,
        avg_margin_without=record.result.avg_margin_without,
        average_minutes=record.minutes.average_minutes,
        total_minutes=record.minutes.total_minutes,
        raw_wowy_score=record.result.value if include_raw_wowy_score else None,
    )
