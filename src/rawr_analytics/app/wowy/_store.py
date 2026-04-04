from __future__ import annotations

from rawr_analytics.data.metric_store import WowyPlayerSeasonValueRow
from rawr_analytics.metrics.wowy.analysis import WowyPlayerValue
from rawr_analytics.metrics.wowy.records import WowyPlayerSeasonValue
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary


def build_wowy_value_from_store_row(row: WowyPlayerSeasonValueRow) -> WowyPlayerSeasonValue:
    return WowyPlayerSeasonValue(
        season_id=row.season_id,
        player=PlayerSummary(
            player_id=row.player_id,
            player_name=row.player_name,
        ),
        minutes=PlayerMinutes(
            average_minutes=row.average_minutes,
            total_minutes=row.total_minutes,
        ),
        result=WowyPlayerValue(
            games_with=row.games_with,
            games_without=row.games_without,
            avg_margin_with=row.avg_margin_with,
            avg_margin_without=row.avg_margin_without,
            value=row.value,
            raw_value=row.raw_wowy_score,
        ),
    )
