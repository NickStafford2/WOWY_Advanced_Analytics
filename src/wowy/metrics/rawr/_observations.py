from __future__ import annotations

from collections import defaultdict

from wowy.metrics.rawr.models import RawrObservation

_LINEUP_WEIGHT_SUM = 5.0


def build_minute_weights(player_minutes: dict[int, float]) -> dict[int, float]:
    total_minutes = sum(player_minutes.values())
    if total_minutes <= 0.0:
        raise ValueError("Expected positive total team minutes for RAWR observation")

    return {
        player_id: (minutes / total_minutes) * _LINEUP_WEIGHT_SUM
        for player_id, minutes in player_minutes.items()
    }


def count_player_games(observations: list[RawrObservation]) -> dict[int, int]:
    games_by_player: dict[int, int] = defaultdict(int)
    for observation in observations:
        for player_id in observation.player_weights:
            games_by_player[player_id] += 1
    return dict(games_by_player)


def count_player_season_games(
    observations: list[RawrObservation],
) -> dict[tuple[str, int], int]:
    games_by_player_season: dict[tuple[str, int], int] = defaultdict(int)
    for observation in observations:
        for player_id in observation.player_weights:
            games_by_player_season[(observation.season, player_id)] += 1
    return dict(games_by_player_season)


def count_player_season_minutes(
    observations: list[RawrObservation],
) -> dict[tuple[str, int], float]:
    minutes_by_player_season: dict[tuple[str, int], float] = {}
    for observation in observations:
        if observation.player_minutes is None:
            continue
        for player_id, minutes in observation.player_minutes.items():
            key = (observation.season, player_id)
            minutes_by_player_season[key] = minutes_by_player_season.get(key, 0.0) + minutes
    return minutes_by_player_season


def build_rawr_player_season_minute_stats(
    games,
    game_players,
) -> dict[tuple[str, int], tuple[float, float]]:
    season_by_game_id = {game.game_id: game.season for game in games}
    totals: dict[tuple[str, int], float] = {}
    counts: dict[tuple[str, int], int] = {}

    for player in game_players:
        season = season_by_game_id.get(player.game_id)
        if season is None or not player.appeared or player.minutes is None or player.minutes <= 0.0:
            continue
        key = (season, player.player_id)
        totals[key] = totals.get(key, 0.0) + player.minutes
        counts[key] = counts.get(key, 0) + 1

    return {key: (totals[key] / counts[key], totals[key]) for key in totals}
