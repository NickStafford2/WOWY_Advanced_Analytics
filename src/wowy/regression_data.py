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

    games_by_id: dict[str, list[NormalizedGameRecord]] = defaultdict(list)
    for game in games:
        games_by_id[game.game_id].append(game)

    observations: list[RegressionObservation] = []
    for game_id, game_rows in sorted(games_by_id.items()):
        if len(game_rows) != 2:
            raise ValueError(
                f"Expected exactly two team rows for game {game_id!r}, found {len(game_rows)}"
            )

        home_games = [game for game in game_rows if game.is_home]
        away_games = [game for game in game_rows if not game.is_home]
        if len(home_games) != 1 or len(away_games) != 1:
            raise ValueError(
                f"Expected one home row and one away row for game {game_id!r}"
            )

        home_game = home_games[0]
        away_game = away_games[0]
        home_player_ids = player_ids_by_game_team.get((game_id, home_game.team), set())
        away_player_ids = player_ids_by_game_team.get((game_id, away_game.team), set())
        if not home_player_ids:
            raise ValueError(
                f"No appeared players found for game {game_id!r} and team {home_game.team!r}"
            )
        if not away_player_ids:
            raise ValueError(
                f"No appeared players found for game {game_id!r} and team {away_game.team!r}"
            )

        player_weights: dict[int, float] = {}
        for player_id in home_player_ids:
            player_weights[player_id] = 1.0
        for player_id in away_player_ids:
            player_weights[player_id] = -1.0

        observations.append(
            RegressionObservation(
                game_id=game_id,
                season=home_game.season,
                game_date=home_game.game_date,
                home_team=home_game.team,
                away_team=away_game.team,
                margin=home_game.margin,
                player_weights=player_weights,
            )
        )

    return observations, player_names


def count_player_games(observations: list[RegressionObservation]) -> dict[int, int]:
    games_by_player: dict[int, int] = defaultdict(int)
    for observation in observations:
        for player_id in observation.player_weights:
            games_by_player[player_id] += 1
    return dict(games_by_player)
