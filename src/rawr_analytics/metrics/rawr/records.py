from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.rawr._observations import count_player_season_games
from rawr_analytics.metrics.rawr._shrinkage import RawrShrinkageMode
from rawr_analytics.metrics.rawr.analysis import RawrValue, fit_player_rawr
from rawr_analytics.metrics.rawr.inputs import (
    RawrRequest,
    RawrSeasonInput,
    passes_minute_filters,
    validate_request,
)
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class RawrPlayerSeasonRecord:
    season: Season
    player: PlayerSummary
    minutes: PlayerMinutes
    result: RawrValue

    @staticmethod
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

    @staticmethod
    def prepare_rawr_player_season_records(
        *,
        season_inputs: list[RawrSeasonInput],
        min_games: int,
        ridge_alpha: float,
        shrinkage_mode: RawrShrinkageMode,
        shrinkage_strength: float,
        shrinkage_minute_scale: float,
        min_average_minutes: float | None = None,
        min_total_minutes: float | None = None,
    ) -> list[RawrPlayerSeasonRecord]:
        return RawrPlayerSeasonRecord.build_player_season_records(
            RawrRequest(
                season_inputs=season_inputs,
                min_games=min_games,
                ridge_alpha=ridge_alpha,
                shrinkage_mode=shrinkage_mode,
                shrinkage_strength=shrinkage_strength,
                shrinkage_minute_scale=shrinkage_minute_scale,
                min_average_minutes=min_average_minutes,
                min_total_minutes=min_total_minutes,
            )
        )


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
