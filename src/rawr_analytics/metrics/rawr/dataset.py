from __future__ import annotations

from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
)
from rawr_analytics.metrics.rawr.inputs import RawrRequest, RawrSeasonInput
from rawr_analytics.metrics.rawr.records import RawrPlayerSeasonRecord, build_player_season_records


def build_rawr_custom_query_result(
    *,
    season_inputs: list[RawrSeasonInput],
    min_games: int,
    ridge_alpha: float,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> list[RawrPlayerSeasonRecord]:
    return build_player_season_records(
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
    )
