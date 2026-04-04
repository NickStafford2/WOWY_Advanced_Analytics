from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics._validation import validate_top_n_and_minutes
from rawr_analytics.metrics.rawr._observations import RawrObservation
from rawr_analytics.metrics.rawr._shrinkage import RawrShrinkageMode
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class RawrPlayerContext:
    player: PlayerSummary
    minutes: PlayerMinutes


@dataclass(frozen=True)
class RawrSeasonInput:
    season: Season
    observations: list[RawrObservation]
    players: list[RawrPlayerContext]


@dataclass(frozen=True)
class RawrRequest:
    season_inputs: list[RawrSeasonInput]
    min_games: int
    ridge_alpha: float
    shrinkage_mode: RawrShrinkageMode = RawrShrinkageMode.UNIFORM
    shrinkage_strength: float = 1.0
    shrinkage_minute_scale: float = 48.0
    min_average_minutes: float | None = None
    min_total_minutes: float | None = None


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


def validate_request(request: RawrRequest) -> None:
    validate_filters(
        request.min_games,
        request.ridge_alpha,
        shrinkage_mode=request.shrinkage_mode,
        shrinkage_strength=request.shrinkage_strength,
        shrinkage_minute_scale=request.shrinkage_minute_scale,
        min_average_minutes=request.min_average_minutes,
        min_total_minutes=request.min_total_minutes,
    )
    for season_input in request.season_inputs:
        _validate_season_input(season_input)


def _validate_season_input(season_input: RawrSeasonInput) -> None:
    player_ids = {player.player.player_id for player in season_input.players}
    if len(player_ids) != len(season_input.players):
        raise ValueError(f"RAWR season {season_input.season!r} has duplicate player contexts")
    for observation in season_input.observations:
        unknown_player_ids = sorted(
            player_id
            for player_id in observation.player_weights
            if player_id not in player_ids
        )
        if unknown_player_ids:
            raise ValueError(
                f"RAWR season {season_input.season!r} references unknown players "
                f"{unknown_player_ids!r}"
            )


def passes_minute_filters(
    player: RawrPlayerContext,
    *,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> bool:
    if min_average_minutes is not None and (
        player.minutes.average_minutes is None
        or player.minutes.average_minutes < min_average_minutes
    ):
        return False
    return min_total_minutes is None or (
        player.minutes.total_minutes is not None
        and player.minutes.total_minutes >= min_total_minutes
    )
