from __future__ import annotations

from collections import defaultdict

from rawr_analytics.metrics.rawr._observations import build_minute_weights
from rawr_analytics.metrics.rawr.models import (
    RawrObservation,
    RawrPlayerEstimate,
    RawrResult,
)
from rawr_analytics.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.minutes import passes_minute_filters
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

__all__ = [
    "attach_minute_stats_to_result",
    "build_rawr_observations",
    "filter_rawr_estimates_by_minutes",
    "filter_rawr_scope",
]


def build_rawr_observations(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> tuple[list[RawrObservation], dict[int, str]]:
    player_minutes_by_game_team: dict[tuple[str, Team], dict[int, float]] = defaultdict(dict)
    player_names: dict[int, str] = {}

    for player in game_players:
        player_names[player.player_id] = player.player_name
        if not player.appeared:
            continue
        minutes = player.minutes
        if minutes is None or minutes <= 0.0:
            raise ValueError(
                f"Missing positive minutes for appeared player {player.player_id!r} "
                f"in game {player.game_id!r} and team {player.team.current.abbreviation!r}"
            )
        player_minutes_by_game_team[(player.game_id, player.team)][player.player_id] = minutes

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
        home_player_minutes = player_minutes_by_game_team.get((game_id, home_game.team), {})
        away_player_minutes = player_minutes_by_game_team.get((game_id, away_game.team), {})
        if not home_player_minutes:
            raise ValueError(
                f"No appeared players found for game {game_id!r} and team "
                f"{home_game.team.abbreviation(season=home_game.season)!r}"
            )
        if not away_player_minutes:
            raise ValueError(
                f"No appeared players found for game {game_id!r} and team "
                f"{away_game.team.abbreviation(season=away_game.season)!r}"
            )

        player_weights: dict[int, float] = {}
        for player_id, weight in build_minute_weights(home_player_minutes).items():
            player_weights[player_id] = weight
        for player_id, weight in build_minute_weights(away_player_minutes).items():
            player_weights[player_id] = -weight

        observations.append(
            RawrObservation(
                game_id=game_id,
                season=home_game.season,
                game_date=home_game.game_date,
                margin=home_game.margin,
                player_weights=player_weights,
                player_minutes=home_player_minutes | away_player_minutes,
                home_team=home_game.team,
                away_team=away_game.team,
            )
        )

    return observations, player_names


def attach_minute_stats_to_result(
    result: RawrResult,
    player_minute_stats: dict[tuple[Season, int], tuple[float, float]] | None,
) -> RawrResult:
    if player_minute_stats is None:
        return result

    estimates = [
        RawrPlayerEstimate(
            season=estimate.season,
            player_id=estimate.player_id,
            player_name=estimate.player_name,
            games=estimate.games,
            average_minutes=player_minute_stats.get(
                (estimate.season, estimate.player_id),
                (None, None),
            )[0],
            total_minutes=player_minute_stats.get(
                (estimate.season, estimate.player_id),
                (None, None),
            )[1],
            coefficient=estimate.coefficient,
        )
        for estimate in result.estimates
    ]
    return RawrResult(
        observations=result.observations,
        players=result.players,
        intercept=result.intercept,
        home_court_advantage=result.home_court_advantage,
        estimates=estimates,
    )


def filter_rawr_estimates_by_minutes(
    result: RawrResult,
    player_minute_stats: dict[tuple[Season, int], tuple[float, float]] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> RawrResult:
    if player_minute_stats is None:
        return result
    if min_average_minutes is None and min_total_minutes is None:
        return result

    filtered_estimates = [
        estimate
        for estimate in result.estimates
        if passes_minute_filters(
            player_minute_stats.get((estimate.season, estimate.player_id)),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
    ]

    return RawrResult(
        observations=result.observations,
        players=result.players,
        intercept=result.intercept,
        home_court_advantage=result.home_court_advantage,
        estimates=filtered_estimates,
    )


def filter_rawr_scope(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    teams: list[Team] | None,
    seasons: list[Season] | None,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    if not teams and not seasons:
        return games, game_players

    normalized_teams = set(teams or [])
    normalized_seasons = set(seasons or [])
    selected_game_ids = {
        game.game_id
        for game in games
        if (not normalized_seasons or game.season in normalized_seasons)
        and (not normalized_teams or game.team in normalized_teams)
    }
    if not selected_game_ids:
        raise ValueError("No games matched the requested RAWR scope")

    filtered_games = [game for game in games if game.game_id in selected_game_ids]
    filtered_game_players = [
        player for player in game_players if player.game_id in selected_game_ids
    ]
    return filtered_games, filtered_game_players
