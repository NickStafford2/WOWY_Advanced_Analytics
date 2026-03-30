from __future__ import annotations

from rawr_analytics.data.game_cache.repository import load_normalized_scope_records_from_db
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.metrics.rawr._observations import build_rawr_player_season_minute_stats
from rawr_analytics.metrics.rawr.analysis import fit_player_rawr
from rawr_analytics.metrics.rawr.inputs import (
    attach_minute_stats_to_result,
    build_rawr_observations,
    filter_rawr_estimates_by_minutes,
    filter_rawr_scope,
)
from rawr_analytics.metrics.rawr.models import RawrPlayerSeasonRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

__all__ = [
    "prepare_rawr_player_season_records",
]


def prepare_rawr_player_season_records(
    *,
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType,
    min_games: int,
    ridge_alpha: float,
    shrinkage_mode: str,
    shrinkage_strength: float,
    shrinkage_minute_scale: float,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> list[RawrPlayerSeasonRecord]:
    team_seasons = resolve_team_seasons(
        teams,
        seasons,
        season_type=season_type,
    )
    teams_by_season: dict[Season, list[Team]] = {}
    for scope in team_seasons:
        teams_by_season.setdefault(scope.season, []).append(scope.team)
    from rawr_analytics.metrics.rawr.data import select_complete_rawr_scope_seasons

    complete_seasons = set(
        select_complete_rawr_scope_seasons(
            teams=teams,
            seasons=seasons,
            season_type=season_type,
        )
    )
    records: list[RawrPlayerSeasonRecord] = []

    for season in sorted(teams_by_season, key=lambda item: item.id):
        if season not in complete_seasons:
            continue
        requested_team_seasons = resolve_team_seasons(
            teams_by_season[season],
            [season],
            season_type=season_type,
        )
        if not requested_team_seasons:
            continue
        games, _ = load_normalized_scope_records_from_db(requested_team_seasons)
        team_seasons = list(requested_team_seasons)
        for scope in {
            TeamSeasonScope(team=game.opponent_team, season=game.season) for game in games
        }:
            if scope not in team_seasons:
                team_seasons.append(scope)
        games, game_players = load_normalized_scope_records_from_db(team_seasons)
        try:
            games, game_players = filter_rawr_scope(
                games,
                game_players,
                teams=[scope.team for scope in requested_team_seasons],
                seasons=[season],
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
