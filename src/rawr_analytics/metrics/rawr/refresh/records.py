from __future__ import annotations

from rawr_analytics.metrics._player_context import PlayerSeasonFilters
from rawr_analytics.metrics.rawr._calc_vars import RawrCalcVars
from rawr_analytics.metrics.rawr.cache import load_rawr_records
from rawr_analytics.metrics.rawr.calculate.inputs import build_rawr_request_from_calc_vars
from rawr_analytics.metrics.rawr.calculate.records import (
    RawrPlayerSeasonRecord,
    build_player_season_records,
)


def build_rawr_refresh_records(
    *,
    calc_vars: RawrCalcVars,
) -> list[RawrPlayerSeasonRecord]:
    assert calc_vars.seasons, "RAWR store record builds require explicit non-empty seasons"
    season_games, season_game_players = load_rawr_records(
        teams=calc_vars.teams,
        seasons=calc_vars.seasons,
    )
    request = build_rawr_request_from_calc_vars(
        calc_vars=calc_vars,
        season_games=season_games,
        season_game_players=season_game_players,
        filters=PlayerSeasonFilters(min_average_minutes=None, min_total_minutes=None),
    )
    return build_player_season_records(request)


__all__ = [
    "build_rawr_refresh_records",
]
