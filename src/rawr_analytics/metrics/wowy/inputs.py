from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics._player_context import PlayerSeasonContext
from rawr_analytics.metrics._validation import validate_top_n_and_minutes
from rawr_analytics.metrics.wowy.analysis import WowyGame
from rawr_analytics.shared.season import Season


@dataclass(frozen=True)
class WowySeasonInputDTO:
    season: Season
    games: list[WowyGame]
    players_by_id: dict[int, PlayerSeasonContext]


@dataclass(frozen=True)
class WowyRequestDTO:
    season_inputs: list[WowySeasonInputDTO]
    min_games_with: int
    min_games_without: int
    min_average_minutes: float | None = None
    min_total_minutes: float | None = None


def build_wowy_request(
    *,
    season_inputs: list[WowySeasonInputDTO],
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> WowyRequestDTO:
    return WowyRequestDTO(
        season_inputs=season_inputs,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )


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


def validate_request(request: WowyRequestDTO) -> None:
    validate_filters(
        request.min_games_with,
        request.min_games_without,
        min_average_minutes=request.min_average_minutes,
        min_total_minutes=request.min_total_minutes,
    )
    for season_input in request.season_inputs:
        _validate_season_input(season_input)


def _validate_season_input(season_input: WowySeasonInputDTO) -> None:
    player_ids = set(season_input.players_by_id)
    for game in season_input.games:
        unknown_player_ids = sorted(game.players - player_ids)
        if unknown_player_ids:
            raise ValueError(
                f"WOWY season {season_input.season!r} references unknown players "
                f"{unknown_player_ids!r}"
            )
