from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.rawr._analysis import fit_player_rawr
from rawr_analytics.metrics.rawr._observations import count_player_season_games
from rawr_analytics.metrics.rawr.inputs import (
    RawrRequestDTO,
    RawrSeasonInputDTO,
    validate_request,
)
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class RawrPlayerSeasonRecord:
    season: Season
    player: PlayerSummary
    minutes: PlayerMinutes
    games: int
    coefficient: float


def build_player_season_records(request: RawrRequestDTO) -> list[RawrPlayerSeasonRecord]:
    validate_request(request)
    records: list[RawrPlayerSeasonRecord] = []
    for season_input in sorted(
        request.season_inputs, key=lambda item: item.season.year_string_nba_api
    ):
        records.extend(_build_season_records(season_input, request=request))
    records.sort(
        key=lambda record: (
            record.season.year_string_nba_api,
            record.coefficient,
            record.player.player_name,
        ),
        reverse=True,
    )
    return records


def _build_season_records(
    season_input: RawrSeasonInputDTO,
    *,
    request: RawrRequestDTO,
) -> list[RawrPlayerSeasonRecord]:
    player_contexts = season_input.players_by_id
    coefficients_by_player_id = fit_player_rawr(
        season_input.observations,
        season=season_input.season,
        min_games=request.min_games,
        ridge_alpha=request.ridge_alpha,
        shrinkage_mode=request.shrinkage_mode,
        shrinkage_strength=request.shrinkage_strength,
        shrinkage_minute_scale=request.shrinkage_minute_scale,
    )
    games_by_player_id = count_player_season_games(season_input.observations)

    records: list[RawrPlayerSeasonRecord] = []
    for player_id, coefficient in coefficients_by_player_id.items():
        player = player_contexts[player_id]
        if not player.passes_minute_filters(
            min_average_minutes=request.min_average_minutes,
            min_total_minutes=request.min_total_minutes,
        ):
            continue
        records.append(
            RawrPlayerSeasonRecord(
                season=season_input.season,
                player=player.player,
                minutes=player.minutes,
                games=games_by_player_id[player_id],
                coefficient=coefficient,
            )
        )
    return records
