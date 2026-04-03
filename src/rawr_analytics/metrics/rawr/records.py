from __future__ import annotations

from rawr_analytics.metrics.rawr._observations import count_player_season_games
from rawr_analytics.metrics.rawr.analysis import fit_player_rawr
from rawr_analytics.metrics.rawr.inputs import passes_minute_filters, validate_request
from rawr_analytics.metrics.rawr.models import (
    RawrPlayerSeasonRecord,
    RawrRequest,
    RawrSeasonInput,
)


def build_player_season_records(request: RawrRequest) -> list[RawrPlayerSeasonRecord]:
    validate_request(request)
    records: list[RawrPlayerSeasonRecord] = []
    for season_input in sorted(request.season_inputs, key=lambda item: item.season.id):
        records.extend(_build_season_records(season_input, request=request))
    records.sort(
        key=lambda record: (
            record.season.id,
            record.result.coefficient,
            record.player.player_name,
        ),
        reverse=True,
    )
    return records


def _build_season_records(
    season_input: RawrSeasonInput,
    *,
    request: RawrRequest,
) -> list[RawrPlayerSeasonRecord]:
    games_by_player = count_player_season_games(
        season_input.observations,
        season=season_input.season,
    )
    eligible_players = [
        player_key for player_key, games in games_by_player.items() if games >= request.min_games
    ]
    if not eligible_players:
        return []

    player_contexts = {player.player.player_id: player for player in season_input.players}
    result = fit_player_rawr(
        season_input.observations,
        player_names={
            player.player.player_id: player.player.player_name for player in season_input.players
        },
        season=season_input.season,
        min_games=request.min_games,
        ridge_alpha=request.ridge_alpha,
        shrinkage_mode=request.shrinkage_mode,
        shrinkage_strength=request.shrinkage_strength,
        shrinkage_minute_scale=request.shrinkage_minute_scale,
    )

    records: list[RawrPlayerSeasonRecord] = []
    for estimate in result.estimates:
        player = player_contexts[estimate.player.player_id]
        if not passes_minute_filters(
            player,
            min_average_minutes=request.min_average_minutes,
            min_total_minutes=request.min_total_minutes,
        ):
            continue
        records.append(
            RawrPlayerSeasonRecord(
                season=season_input.season,
                player=estimate.player,
                minutes=player.minutes,
                result=estimate.result,
            )
        )
    return records
