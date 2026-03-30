from __future__ import annotations

from rawr_analytics.metrics.wowy.models import WowyPlayerContext, WowyRequest, WowySeasonInput
from rawr_analytics.shared.filters import validate_top_n_and_minutes


def _validate_filters(
    min_games_with: int,
    min_games_without: int,
    *,
    top_n: int | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> None:
    if min_games_with < 0 or min_games_without < 0:
        raise ValueError("Minimum game filters must be non-negative")
    validate_top_n_and_minutes(
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


def _validate_request(request: WowyRequest) -> None:
    _validate_filters(
        request.min_games_with,
        request.min_games_without,
        min_average_minutes=request.min_average_minutes,
        min_total_minutes=request.min_total_minutes,
    )
    for season_input in request.season_inputs:
        _validate_season_input(season_input)


def _validate_season_input(season_input: WowySeasonInput) -> None:
    player_ids = {player.player_id for player in season_input.players}
    if len(player_ids) != len(season_input.players):
        raise ValueError(f"WOWY season {season_input.season!r} has duplicate player contexts")
    for game in season_input.games:
        unknown_player_ids = sorted(game.players - player_ids)
        if unknown_player_ids:
            raise ValueError(
                f"WOWY season {season_input.season!r} references unknown players "
                f"{unknown_player_ids!r}"
            )


def passes_minute_filters(
    player: WowyPlayerContext,
    *,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> bool:
    if min_average_minutes is not None:
        if player.average_minutes is None or player.average_minutes < min_average_minutes:
            return False
    if min_total_minutes is not None:
        if player.total_minutes is None or player.total_minutes < min_total_minutes:
            return False
    return True
