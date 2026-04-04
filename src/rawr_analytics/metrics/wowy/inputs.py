from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics._player_context import PlayerSeasonContext
from rawr_analytics.metrics._validation import validate_top_n_and_minutes
from rawr_analytics.metrics.wowy.analysis import WowyGame
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class WowySeasonInput:
    season: Season
    games: list[WowyGame]
    players: list[PlayerSeasonContext]


@dataclass(frozen=True)
class WowyRequest:
    season_inputs: list[WowySeasonInput]
    min_games_with: int
    min_games_without: int
    min_average_minutes: float | None = None
    min_total_minutes: float | None = None


def validate_filters(
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


def validate_request(request: WowyRequest) -> None:
    validate_filters(
        request.min_games_with,
        request.min_games_without,
        min_average_minutes=request.min_average_minutes,
        min_total_minutes=request.min_total_minutes,
    )
    for season_input in request.season_inputs:
        _validate_season_input(season_input)


def _validate_season_input(season_input: WowySeasonInput) -> None:
    player_ids = {player.player.player_id for player in season_input.players}
    if len(player_ids) != len(season_input.players):
        raise ValueError(f"WOWY season {season_input.season!r} has duplicate player contexts")
    for game in season_input.games:
        unknown_player_ids = sorted(game.players - player_ids)
        if unknown_player_ids:
            raise ValueError(
                f"WOWY season {season_input.season!r} references unknown players "
                f"{unknown_player_ids!r}"
            )
