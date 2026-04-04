from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrObservation:
    game_id: str
    game_date: str
    margin: float
    home_team: Team
    away_team: Team
    player_weights: dict[int, float]
    player_minutes: dict[int, float] | None = None


_LINEUP_WEIGHT_SUM = 5.0


def _build_minute_weights(player_minutes: dict[int, float]) -> dict[int, float]:
    total_minutes = sum(player_minutes.values())
    if total_minutes <= 0.0:
        raise ValueError("Expected positive total team minutes for RAWR observation")

    return {
        player_id: (minutes / total_minutes) * _LINEUP_WEIGHT_SUM
        for player_id, minutes in player_minutes.items()
    }


def count_player_season_games(
    observations: list[RawrObservation],
) -> dict[int, int]:
    games_by_player: dict[int, int] = defaultdict(int)
    for observation in observations:
        for player_id in observation.player_weights:
            games_by_player[player_id] += 1
    return dict(games_by_player)


def count_player_season_minutes(
    observations: list[RawrObservation],
) -> dict[int, float]:
    minutes_by_player: dict[int, float] = {}
    for observation in observations:
        if observation.player_minutes is None:
            continue
        for player_id, minutes in observation.player_minutes.items():
            minutes_by_player[player_id] = minutes_by_player.get(player_id, 0.0) + minutes
    return minutes_by_player


def build_rawr_observations(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> tuple[list[RawrObservation], dict[int, str]]:
    player_minutes_by_game_team: dict[tuple[str, int], dict[int, float]] = defaultdict(dict)
    player_names: dict[int, str] = {}

    for player in game_players:
        player_names[player.player.player_id] = player.player.player_name
        if not player.has_positive_minutes():
            continue
        minutes = player.minutes
        assert minutes is not None
        player_minutes_by_game_team[(player.game_id, player.team.team_id)][
            player.player.player_id
        ] = minutes

    games_by_id: dict[str, list[NormalizedGameRecord]] = defaultdict(list)
    for game in games:
        games_by_id[game.game_id].append(game)

    observations: list[RawrObservation] = []
    for game_id, game_rows in sorted(games_by_id.items()):
        if len(game_rows) != 2:
            raise ValueError(
                f"Expected exactly two team rows for game {game_id!r}, found {len(game_rows)}"
            )
        home_games = [game for game in game_rows if game.is_home]
        away_games = [game for game in game_rows if not game.is_home]
        if len(home_games) != 1 or len(away_games) != 1:
            raise ValueError(f"Expected one home row and one away row for game {game_id!r}")

        home_game = home_games[0]
        away_game = away_games[0]
        home_player_minutes = player_minutes_by_game_team.get((game_id, home_game.team.team_id), {})
        away_player_minutes = player_minutes_by_game_team.get((game_id, away_game.team.team_id), {})
        if not home_player_minutes:
            raise ValueError(
                f"No players with positive minutes found for game {game_id!r} and team "
                f"{home_game.team.abbreviation(season=home_game.season)!r}"
            )
        if not away_player_minutes:
            raise ValueError(
                f"No players with positive minutes found for game {game_id!r} and team "
                f"{away_game.team.abbreviation(season=away_game.season)!r}"
            )

        player_weights: dict[int, float] = {}
        for player_id, weight in _build_minute_weights(home_player_minutes).items():
            player_weights[player_id] = weight
        for player_id, weight in _build_minute_weights(away_player_minutes).items():
            player_weights[player_id] = -weight

        observations.append(
            RawrObservation(
                game_id=game_id,
                game_date=home_game.game_date,
                margin=home_game.margin,
                player_weights=player_weights,
                player_minutes=home_player_minutes | away_player_minutes,
                home_team=home_game.team,
                away_team=away_game.team,
            )
        )
    return observations, player_names


def build_rawr_player_season_minute_stats(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> dict[tuple[Season, int], tuple[float, float]]:
    season_by_game_id = {game.game_id: game.season for game in games}
    totals: dict[tuple[Season, int], float] = {}
    counts: dict[tuple[Season, int], int] = {}

    for player in game_players:
        season = season_by_game_id.get(player.game_id)
        if season is None or not player.has_positive_minutes():
            continue
        assert player.minutes is not None
        key = (season, player.player.player_id)
        totals[key] = totals.get(key, 0.0) + player.minutes
        counts[key] = counts.get(key, 0) + 1

    return {key: (totals[key] / counts[key], totals[key]) for key in totals}
