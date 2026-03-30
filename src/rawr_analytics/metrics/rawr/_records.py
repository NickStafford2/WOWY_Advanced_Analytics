from __future__ import annotations

from rawr_analytics.metrics.rawr._inputs import passes_minute_filters
from rawr_analytics.metrics.rawr.analysis import fit_player_rawr
from rawr_analytics.metrics.rawr.models import (
    RawrPlayerSeasonRecord,
    RawrRequest,
    RawrSeasonInput,
)


def build_player_season_records(request: RawrRequest) -> list[RawrPlayerSeasonRecord]:
    records: list[RawrPlayerSeasonRecord] = []
    for season_input in sorted(request.season_inputs, key=lambda item: item.season.id):
        records.extend(_build_season_records(season_input, request=request))
    records.sort(
        key=lambda record: (record.season, record.coefficient, record.player_name),
        reverse=True,
    )
    return records


def _build_season_records(
    season_input: RawrSeasonInput,
    *,
    request: RawrRequest,
) -> list[RawrPlayerSeasonRecord]:
    player_contexts = {player.player_id: player for player in season_input.players}
    result = fit_player_rawr(
        season_input.observations,
        player_names={player.player_id: player.player_name for player in season_input.players},
        season=season_input.season,
        min_games=request.min_games,
        ridge_alpha=request.ridge_alpha,
        shrinkage_mode=request.shrinkage_mode,
        shrinkage_strength=request.shrinkage_strength,
        shrinkage_minute_scale=request.shrinkage_minute_scale,
    )

    records: list[RawrPlayerSeasonRecord] = []
    for estimate in result.estimates:
        player = player_contexts[estimate.player_id]
        if not passes_minute_filters(
            player,
            min_average_minutes=request.min_average_minutes,
            min_total_minutes=request.min_total_minutes,
        ):
            continue
        records.append(
            RawrPlayerSeasonRecord(
                season=season_input.season,
                player_id=estimate.player_id,
                player_name=estimate.player_name,
                games=estimate.games,
                average_minutes=player.average_minutes,
                total_minutes=player.total_minutes,
                coefficient=estimate.coefficient,
            )
        )
    return records
