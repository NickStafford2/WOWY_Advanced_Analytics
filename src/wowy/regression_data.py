from __future__ import annotations

from collections import defaultdict

from wowy.regression_types import RegressionObservation
from wowy.types import NormalizedGamePlayerRecord, NormalizedGameRecord

LINEUP_WEIGHT_SUM = 5.0


def build_regression_observations(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> tuple[list[RegressionObservation], dict[int, str]]:
    player_minutes_by_game_team: dict[tuple[str, str], dict[int, float]] = defaultdict(dict)
    player_names: dict[int, str] = {}

    for player in game_players:
        player_names[player.player_id] = player.player_name
        if not player.appeared:
            continue
        minutes = player.minutes
        if minutes is None or minutes <= 0.0:
            raise ValueError(
                f"Missing positive minutes for appeared player {player.player_id!r} "
                f"in game {player.game_id!r} and team {player.team!r}"
            )
        player_minutes_by_game_team[(player.game_id, player.team)][player.player_id] = minutes

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
        home_player_minutes = player_minutes_by_game_team.get((game_id, home_game.team), {})
        away_player_minutes = player_minutes_by_game_team.get((game_id, away_game.team), {})
        if not home_player_minutes:
            raise ValueError(
                f"No appeared players found for game {game_id!r} and team {home_game.team!r}"
            )
        if not away_player_minutes:
            raise ValueError(
                f"No appeared players found for game {game_id!r} and team {away_game.team!r}"
            )

        player_weights: dict[int, float] = {}
        for player_id, weight in build_minute_weights(home_player_minutes).items():
            player_weights[player_id] = weight
        for player_id, weight in build_minute_weights(away_player_minutes).items():
            player_weights[player_id] = -weight

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


def build_minute_weights(player_minutes: dict[int, float]) -> dict[int, float]:
    total_minutes = sum(player_minutes.values())
    if total_minutes <= 0.0:
        raise ValueError("Expected positive total team minutes for regression observation")

    return {
        player_id: (minutes / total_minutes) * LINEUP_WEIGHT_SUM
        for player_id, minutes in player_minutes.items()
    }


def count_player_games(observations: list[RegressionObservation]) -> dict[int, int]:
    games_by_player: dict[int, int] = defaultdict(int)
    for observation in observations:
        for player_id in observation.player_weights:
            games_by_player[player_id] += 1
    return dict(games_by_player)
