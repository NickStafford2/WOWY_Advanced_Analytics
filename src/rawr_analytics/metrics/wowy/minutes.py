from __future__ import annotations

from rawr_analytics.data.game_cache.repository import load_normalized_scope_records_from_db
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.metrics._scope_values import season_ids, team_ids
from rawr_analytics.metrics.wowy.models import WowyPlayerStats
from rawr_analytics.shared.minutes import build_player_minute_stats, passes_minute_filters
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

__all__ = [
    "attach_minute_stats",
    "filter_results_by_minutes",
    "load_player_minute_stats",
    "load_player_season_minute_stats",
]


def load_player_minute_stats(
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType = SeasonType.REGULAR,
) -> dict[int, tuple[float, float]]:
    team_seasons = resolve_team_seasons(
        None,
        season_ids(seasons),
        team_ids=team_ids(teams),
        season_type=season_type,
    )
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")
    _, game_players = load_normalized_scope_records_from_db(team_seasons)
    return build_player_minute_stats(game_players)


def load_player_season_minute_stats(
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType = SeasonType.REGULAR,
) -> dict[tuple[Season, int], tuple[float, float]]:
    totals: dict[tuple[Season, int], float] = {}
    counts: dict[tuple[Season, int], int] = {}

    team_seasons = resolve_team_seasons(
        None,
        season_ids(seasons),
        team_ids=team_ids(teams),
        season_type=season_type,
    )
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")
    games, game_players = load_normalized_scope_records_from_db(team_seasons)
    seasons_by_game_id = {game.game_id: game.season for game in games}
    for player in game_players:
        season = seasons_by_game_id.get(player.game_id)
        if season is None or not player.appeared or player.minutes is None or player.minutes <= 0.0:
            continue
        key = (season, player.player_id)
        totals[key] = totals.get(key, 0.0) + player.minutes
        counts[key] = counts.get(key, 0) + 1

    return {key: (totals[key] / counts[key], totals[key]) for key in totals}


def filter_results_by_minutes(
    results: dict[int, WowyPlayerStats],
    player_minute_stats: dict[int, tuple[float, float]] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[int, WowyPlayerStats]:
    if player_minute_stats is None:
        return results
    if min_average_minutes is None and min_total_minutes is None:
        return results

    return {
        player_id: stats
        for player_id, stats in results.items()
        if passes_minute_filters(
            player_minute_stats.get(player_id),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
    }


def attach_minute_stats(
    results: dict[int, WowyPlayerStats],
    player_minute_stats: dict[int, tuple[float, float]] | None,
) -> dict[int, WowyPlayerStats]:
    if player_minute_stats is None:
        return results

    updated = {}
    for player_id, stats in results.items():
        average_minutes, total_minutes = player_minute_stats.get(
            player_id,
            (None, None),
        )
        updated[player_id] = WowyPlayerStats(
            games_with=stats.games_with,
            games_without=stats.games_without,
            avg_margin_with=stats.avg_margin_with,
            avg_margin_without=stats.avg_margin_without,
            wowy_score=stats.wowy_score,
            average_minutes=average_minutes,
            total_minutes=total_minutes,
        )
    return updated
