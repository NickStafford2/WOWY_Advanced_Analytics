from __future__ import annotations

from pathlib import Path

from rawr_analytics.data.player_metrics_db.constants import DEFAULT_PLAYER_METRICS_DB_PATH
from rawr_analytics.metrics.wowy.models import WowyPlayerStats
from rawr_analytics.nba.prepare import load_normalized_scope_records
from rawr_analytics.shared.minutes import build_player_minute_stats, passes_minute_filters

__all__ = [
    "attach_minute_stats",
    "filter_results_by_minutes",
    "load_player_minute_stats",
    "load_player_season_minute_stats",
]


def load_player_minute_stats(
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str = "Regular Season",
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    team_ids: list[int] | None = None,
) -> dict[int, tuple[float, float]]:
    _, game_players = load_normalized_scope_records(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
        include_opponents_for_team_scope=False,
    )
    return build_player_minute_stats(game_players)


def load_player_season_minute_stats(
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str = "Regular Season",
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    team_ids: list[int] | None = None,
) -> dict[tuple[str, int], tuple[float, float]]:
    totals: dict[tuple[str, int], float] = {}
    counts: dict[tuple[str, int], int] = {}

    games, game_players = load_normalized_scope_records(
        teams=teams,
        seasons=seasons,
        team_ids=team_ids,
        season_type=season_type,
        player_metrics_db_path=player_metrics_db_path,
        include_opponents_for_team_scope=False,
    )
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
