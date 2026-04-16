from __future__ import annotations

from rawr_analytics.metrics._player_context import PlayerSeasonFilters
from rawr_analytics.metrics.wowy._calc_vars import WowyCalcVars
from rawr_analytics.metrics.wowy.cache import load_wowy_records
from rawr_analytics.metrics.wowy.calculate.inputs import build_wowy_season_inputs
from rawr_analytics.metrics.wowy.calculate.records import (
    WowyPlayerSeasonRecord,
    prepare_wowy_player_season_records_from_calc_vars,
)


def build_wowy_refresh_records(
    *,
    calc_vars: WowyCalcVars,
) -> list[WowyPlayerSeasonRecord]:
    assert calc_vars.seasons, "WOWY refresh record builds require explicit non-empty seasons"
    games, game_players = load_wowy_records(teams=calc_vars.teams, seasons=calc_vars.seasons)
    season_inputs = build_wowy_season_inputs(games=games, game_players=game_players)
    return prepare_wowy_player_season_records_from_calc_vars(
        calc_vars=calc_vars,
        season_inputs=season_inputs,
        filters=PlayerSeasonFilters(min_average_minutes=None, min_total_minutes=None),
    )


__all__ = [
    "build_wowy_refresh_records",
]
