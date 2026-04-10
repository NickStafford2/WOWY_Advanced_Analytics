from __future__ import annotations

from rawr_analytics.metrics.rawr.cache import load_rawr_records
from rawr_analytics.metrics.rawr.calculate.inputs import build_rawr_request
from rawr_analytics.metrics.rawr.calculate.records import (
    RawrPlayerSeasonRecord,
    build_player_season_records,
)
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
)
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def build_rawr_refresh_records(
    *,
    seasons: list[Season],
    teams: list[Team],
    ridge_alpha: float,
) -> list[RawrPlayerSeasonRecord]:
    assert seasons, "RAWR store record builds require explicit non-empty seasons"
    season_games, season_game_players = load_rawr_records(
        teams=teams,
        seasons=seasons,
    )
    request = build_rawr_request(
        season_games=season_games,
        season_game_players=season_game_players,
        min_games=1,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    )
    return build_player_season_records(request)


__all__ = [
    "build_rawr_refresh_records",
]
