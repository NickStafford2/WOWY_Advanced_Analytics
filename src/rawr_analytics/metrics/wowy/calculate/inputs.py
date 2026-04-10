from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from rawr_analytics.metrics._player_context import PlayerSeasonContext
from rawr_analytics.metrics._validation import validate_top_n_and_minutes
from rawr_analytics.metrics.wowy.calculate._analysis import WowyGame
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
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


def build_wowy_season_inputs(
    *,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> list[WowySeasonInputDTO]:
    player_names = _build_player_names(game_players)
    minute_stats = _build_wowy_player_season_minute_stats(games, game_players)
    games_by_season = _derive_wowy_games_by_season(games, game_players)

    season_inputs: list[WowySeasonInputDTO] = []
    for season in sorted(games_by_season, key=lambda item: item.year_string_nba_api):
        player_ids = sorted(
            {player_id for game in games_by_season[season] for player_id in game.players}
        )
        season_inputs.append(
            WowySeasonInputDTO(
                season=season,
                games=games_by_season[season],
                players_by_id={
                    player_id: PlayerSeasonContext(
                        player=PlayerSummary(
                            player_id=player_id,
                            player_name=player_names.get(player_id, str(player_id)),
                        ),
                        minutes=_player_minutes(minute_stats, season, player_id),
                    )
                    for player_id in player_ids
                },
            )
        )
    return season_inputs


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


def _build_player_names(game_players: list[NormalizedGamePlayerRecord]) -> dict[int, str]:
    return {player.player.player_id: player.player.player_name for player in game_players}


def _derive_wowy_games_by_season(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> dict[Season, list[WowyGame]]:
    players_by_game_team: dict[tuple[str, int], set[int]] = defaultdict(set)
    for player in game_players:
        if not player.appeared:
            continue
        players_by_game_team[(player.game_id, player.team.team_id)].add(player.player.player_id)

    games_by_season: dict[Season, list[WowyGame]] = defaultdict(list)
    for game in games:
        players = players_by_game_team.get((game.game_id, game.team.team_id), set())
        if not players:
            raise ValueError(
                f"No appeared players found for game {game.game_id!r} and team "
                f"{game.team.abbreviation(season=game.season)!r}"
            )
        games_by_season[game.season].append(
            WowyGame(
                game_id=game.game_id,
                margin=game.margin,
                players=frozenset(players),
                team=game.team,
            )
        )
    return dict(games_by_season)


def _build_wowy_player_season_minute_stats(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> dict[tuple[Season, int], tuple[float, float]]:
    totals: dict[tuple[Season, int], float] = {}
    counts: dict[tuple[Season, int], int] = {}
    season_by_game_id = {game.game_id: game.season for game in games}
    for player in game_players:
        season = season_by_game_id.get(player.game_id)
        if season is None or not player.has_positive_minutes():
            continue
        assert player.minutes is not None
        key = (season, player.player.player_id)
        totals[key] = totals.get(key, 0.0) + player.minutes
        counts[key] = counts.get(key, 0) + 1
    return {key: (totals[key] / counts[key], totals[key]) for key in totals}


def _player_minutes(
    minute_stats: dict[tuple[Season, int], tuple[float, float]],
    season: Season,
    player_id: int,
) -> PlayerMinutes:
    average_minutes, total_minutes = minute_stats.get((season, player_id), (None, None))
    return PlayerMinutes(
        average_minutes=average_minutes,
        total_minutes=total_minutes,
    )
