from __future__ import annotations

from collections import defaultdict

from wowy.regression_types import RegressionObservation
from wowy.types import NormalizedGamePlayerRecord, NormalizedGameRecord


def build_regression_observations(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> tuple[list[RegressionObservation], dict[int, str]]:
    player_ids_by_game_team: dict[tuple[str, str], set[int]] = defaultdict(set)
    player_names: dict[int, str] = {}

    for player in game_players:
        player_names[player.player_id] = player.player_name
        if player.appeared:
            player_ids_by_game_team[(player.game_id, player.team)].add(player.player_id)

    observations: list[RegressionObservation] = []
    for game in games:
        player_ids = player_ids_by_game_team.get((game.game_id, game.team), set())
        if not player_ids:
            raise ValueError(
                f"No appeared players found for game {game.game_id!r} and team {game.team!r}"
            )
        observations.append(
            RegressionObservation(
                game_id=game.game_id,
                season=game.season,
                game_date=game.game_date,
                team=game.team,
                opponent=game.opponent,
                is_home=game.is_home,
                margin=game.margin,
                player_ids=player_ids,
            )
        )

    return observations, player_names


def count_player_games(observations: list[RegressionObservation]) -> dict[int, int]:
    games_by_player: dict[int, int] = defaultdict(int)
    for observation in observations:
        for player_id in observation.player_ids:
            games_by_player[player_id] += 1
    return dict(games_by_player)
