from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from rawr_analytics.metrics._player_context import PlayerSeasonContext, PlayerSeasonFilters
from rawr_analytics.metrics.rawr._calc_vars import RawrCalcVars, RawrEligibility
from rawr_analytics.metrics._validation import validate_top_n_and_minutes
from rawr_analytics.metrics.rawr.calculate._observations import (
    RawrObservation,
    build_rawr_observations,
)
from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class RawrSeasonInputDTO:
    season: Season
    observations: list[RawrObservation]
    players_by_id: dict[int, PlayerSeasonContext]

@dataclass(frozen=True)
class RawrRequestDTO:
    season_inputs: list[RawrSeasonInputDTO]
    eligibility: RawrEligibility
    filters: PlayerSeasonFilters
    ridge_alpha: float
    shrinkage_mode: RawrShrinkageMode = RawrShrinkageMode.UNIFORM
    shrinkage_strength: float = 1.0
    shrinkage_minute_scale: float = 48.0


def build_rawr_request(
    *,
    season_games: dict[Season, list[NormalizedGameRecord]],
    season_game_players: dict[Season, list[NormalizedGamePlayerRecord]],
    eligibility: RawrEligibility,
    filters: PlayerSeasonFilters,
    ridge_alpha: float,
    shrinkage_mode: RawrShrinkageMode = RawrShrinkageMode.UNIFORM,
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
) -> RawrRequestDTO:
    season_inputs: list[RawrSeasonInputDTO] = []
    for season in sorted(season_games, key=lambda item: item.year_string_nba_api):
        season_input = _build_rawr_season_input(
            season=season,
            games=season_games[season],
            game_players=season_game_players.get(season, []),
        )
        if season_input is not None:
            season_inputs.append(season_input)
    return RawrRequestDTO(
        season_inputs=season_inputs,
        eligibility=eligibility,
        filters=filters,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=shrinkage_mode,
        shrinkage_strength=shrinkage_strength,
        shrinkage_minute_scale=shrinkage_minute_scale,
    )


def build_rawr_request_from_calc_vars(
    *,
    calc_vars: RawrCalcVars,
    filters: PlayerSeasonFilters,
    season_games: dict[Season, list[NormalizedGameRecord]],
    season_game_players: dict[Season, list[NormalizedGamePlayerRecord]],
) -> RawrRequestDTO:
    return build_rawr_request(
        season_games=season_games,
        season_game_players=season_game_players,
        eligibility=calc_vars.eligibility,
        filters=filters,
        ridge_alpha=calc_vars.ridge_alpha,
        shrinkage_mode=calc_vars.shrinkage_mode,
        shrinkage_strength=calc_vars.shrinkage_strength,
        shrinkage_minute_scale=calc_vars.shrinkage_minute_scale,
    )


def validate_filters(
    min_games: int,
    ridge_alpha: float,
    *,
    shrinkage_mode: RawrShrinkageMode = RawrShrinkageMode.UNIFORM,
    shrinkage_strength: float = 1.0,
    shrinkage_minute_scale: float = 48.0,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> None:
    if min_games < 0:
        raise ValueError("Minimum games filter must be non-negative")
    if ridge_alpha < 0:
        raise ValueError("Ridge alpha must be non-negative")
    RawrShrinkageMode.validate(
        shrinkage_mode,
        shrinkage_strength,
        shrinkage_minute_scale,
    )
    validate_top_n_and_minutes(
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


def validate_request(request: RawrRequestDTO) -> None:
    validate_filters(
        request.eligibility.min_games,
        request.ridge_alpha,
        shrinkage_mode=request.shrinkage_mode,
        shrinkage_strength=request.shrinkage_strength,
        shrinkage_minute_scale=request.shrinkage_minute_scale,
        min_average_minutes=request.filters.min_average_minutes,
        min_total_minutes=request.filters.min_total_minutes,
    )
    for season_input in request.season_inputs:
        _validate_season_input(season_input)


def _build_rawr_season_input(
    *,
    season: Season,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> RawrSeasonInputDTO | None:
    observations = build_rawr_observations(games, game_players)
    if not observations:
        return None
    player_ids = sorted(
        {player_id for observation in observations for player_id in observation.player_weights}
    )
    return RawrSeasonInputDTO(
        season=season,
        observations=observations,
        players_by_id=_build_players_by_id(
            game_players=game_players,
            player_ids=player_ids,
        ),
    )


def _validate_season_input(season_input: RawrSeasonInputDTO) -> None:
    player_ids = set(season_input.players_by_id)
    for observation in season_input.observations:
        unknown_player_ids = sorted(
            player_id for player_id in observation.player_weights if player_id not in player_ids
        )
        if unknown_player_ids:
            raise ValueError(
                f"RAWR season {season_input.season!r} references unknown players "
                f"{unknown_player_ids!r}"
            )


def _build_players_by_id(
    *,
    game_players: list[NormalizedGamePlayerRecord],
    player_ids: list[int],
) -> dict[int, PlayerSeasonContext]:
    totals_by_player_id: dict[int, float] = defaultdict(float)
    games_by_player_id: dict[int, int] = defaultdict(int)
    players_by_id: dict[int, PlayerSummary] = {}

    for game_player in game_players:
        player_id = game_player.player.player_id
        players_by_id[player_id] = game_player.player
        if not game_player.has_positive_minutes():
            continue
        assert game_player.minutes is not None
        totals_by_player_id[player_id] += game_player.minutes
        games_by_player_id[player_id] += 1

    season_players: dict[int, PlayerSeasonContext] = {}
    for player_id in player_ids:
        total_minutes = totals_by_player_id.get(player_id)
        games = games_by_player_id.get(player_id, 0)
        average_minutes = None if total_minutes is None or games == 0 else total_minutes / games
        season_players[player_id] = PlayerSeasonContext(
            player=players_by_id.get(
                player_id,
                PlayerSummary(player_id=player_id, player_name=str(player_id)),
            ),
            minutes=PlayerMinutes(
                average_minutes=average_minutes,
                total_minutes=total_minutes,
            ),
        )
    return season_players
