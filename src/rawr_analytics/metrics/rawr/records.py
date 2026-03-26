from __future__ import annotations

from pathlib import Path

from rawr_analytics.data.player_metrics_db.constants import DEFAULT_PLAYER_METRICS_DB_PATH
from rawr_analytics.data.scope_resolver import (
    load_normalized_scope_records,
    resolve_team_seasons,
)
from rawr_analytics.metrics.rawr._observations import build_rawr_player_season_minute_stats
from rawr_analytics.metrics.rawr.analysis import fit_player_rawr
from rawr_analytics.metrics.rawr.inputs import (
    attach_minute_stats_to_result,
    build_rawr_observations,
    filter_rawr_estimates_by_minutes,
    filter_rawr_scope,
)
from rawr_analytics.metrics.rawr.models import RawrPlayerSeasonRecord

__all__ = [
    "prepare_rawr_player_season_records",
]


def prepare_rawr_player_season_records(
    *,
    teams: list[str] | None,
    seasons: list[str] | None,
    team_ids: list[int] | None = None,
    season_type: str,
    min_games: int,
    ridge_alpha: float,
    shrinkage_mode: str,
    shrinkage_strength: float,
    shrinkage_minute_scale: float,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
) -> list[RawrPlayerSeasonRecord]:
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        team_ids=team_ids,
        player_metrics_db_path=player_metrics_db_path,
        season_type=season_type,
    )
    teams_by_season: dict[str, list[str]] = {}
    for team_season in team_seasons:
        teams_by_season.setdefault(team_season.season, []).append(team_season.team)
    from rawr_analytics.metrics.rawr.data import select_complete_rawr_scope_seasons

    complete_seasons = set(
        select_complete_rawr_scope_seasons(
            teams=teams,
            seasons=seasons,
            team_ids=team_ids,
            season_type=season_type,
            player_metrics_db_path=player_metrics_db_path,
        )
    )
    records: list[RawrPlayerSeasonRecord] = []

    for season in sorted(teams_by_season):
        if season not in complete_seasons:
            continue
        games, game_players = load_normalized_scope_records(
            teams=sorted(set(teams_by_season[season])),
            seasons=[season],
            season_type=season_type,
            player_metrics_db_path=player_metrics_db_path,
            include_opponents_for_team_scope=True,
        )
        try:
            games, game_players = filter_rawr_scope(
                games,
                game_players,
                teams=sorted(set(teams_by_season[season])),
                seasons=[season],
                team_ids=team_ids,
            )
        except ValueError as exc:
            if str(exc) == "No games matched the requested RAWR scope":
                continue
            raise
        player_minute_stats = build_rawr_player_season_minute_stats(games, game_players)
        observations, player_names = build_rawr_observations(games, game_players)
        try:
            result = fit_player_rawr(
                observations,
                player_names=player_names,
                min_games=min_games,
                ridge_alpha=ridge_alpha,
                shrinkage_mode=shrinkage_mode,
                shrinkage_strength=shrinkage_strength,
                shrinkage_minute_scale=shrinkage_minute_scale,
            )
        except ValueError as exc:
            if str(exc) == "No players met the minimum games requirement":
                continue
            raise
        result = attach_minute_stats_to_result(result, player_minute_stats)
        result = filter_rawr_estimates_by_minutes(
            result,
            player_minute_stats=player_minute_stats,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        for estimate in result.estimates:
            records.append(
                RawrPlayerSeasonRecord(
                    season=season,
                    player_id=estimate.player_id,
                    player_name=estimate.player_name,
                    games=estimate.games,
                    average_minutes=estimate.average_minutes,
                    total_minutes=estimate.total_minutes,
                    coefficient=estimate.coefficient,
                )
            )

    records.sort(
        key=lambda record: (record.season, record.coefficient, record.player_name),
        reverse=True,
    )
    return records
