from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable

from rawr_analytics.data.game_cache.store import load_team_season_cache
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.metrics.rawr.cache_status import list_complete_rawr_seasons
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType, normalize_seasons
from rawr_analytics.shared.team import Team

type RawrSeasonProgressFn = Callable[[int, int, Season], None]


def load_rawr_records(
    *,
    teams: list[Team] | None,
    seasons: list[Season],
    progress_fn: RawrSeasonProgressFn | None = None,
) -> tuple[
    dict[Season, list[NormalizedGameRecord]],
    dict[Season, list[NormalizedGamePlayerRecord]],
]:
    assert seasons, "RAWR record loading requires a non-empty season list"
    requested_team_seasons = resolve_team_seasons(teams, seasons, season_type=season_type)
    if not requested_team_seasons:
        raise ValueError("No cached data matched the requested RAWR scope")

    teams_by_season = _build_teams_by_season(requested_team_seasons)
    complete_seasons = list_complete_rawr_seasons(
        seasons=list(teams_by_season),
    )
    if not complete_seasons:
        raise ValueError("No complete cached seasons matched the requested RAWR scope")

    season_games: dict[Season, list[NormalizedGameRecord]] = {}
    season_game_players: dict[Season, list[NormalizedGamePlayerRecord]] = {}
    sorted_seasons = normalize_seasons(list(teams_by_season))
    assert sorted_seasons is not None, "RAWR team grouping produced no seasons"
    total_seasons = len(sorted_seasons)
    for season_index, season in enumerate(sorted_seasons, start=1):
        if progress_fn is not None:
            progress_fn(season_index, total_seasons, season)
        if season not in complete_seasons:
            continue
        season_records = _load_rawr_season_records(
            teams=teams_by_season[season],
            season=season,
            season_type=season_type,
        )
        if season_records is None:
            continue
        games, game_players = season_records
        season_games[season] = games
        season_game_players[season] = game_players
    return season_games, season_game_players


def _build_teams_by_season(
    team_seasons: list[TeamSeasonScope],
) -> dict[Season, list[Team]]:
    teams_by_season: dict[Season, list[Team]] = defaultdict(list)
    for scope in team_seasons:
        teams_by_season[scope.season].append(scope.team)
    return teams_by_season


def _load_rawr_season_records(
    *,
    teams: list[Team],
    season: Season,
    season_type: SeasonType,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]] | None:
    assert season.season_type == season_type, "RAWR season must match requested season_type"
    requested_team_seasons = resolve_team_seasons(teams, [season], season_type=season_type)
    if not requested_team_seasons:
        return None

    requested_games, _ = load_team_season_cache(requested_team_seasons)
    season_scopes = _expand_rawr_season_scopes(
        requested_team_seasons=requested_team_seasons,
        games=requested_games,
    )
    games, game_players = load_team_season_cache(season_scopes)
    games, game_players = _filter_rawr_scope(
        games,
        game_players,
        teams=[scope.team for scope in requested_team_seasons],
    )
    games, game_players = _exclude_rawr_games_without_positive_minutes(games, game_players)
    if not games:
        return None
    return games, game_players


def _expand_rawr_season_scopes(
    *,
    requested_team_seasons: list[TeamSeasonScope],
    games: list[NormalizedGameRecord],
) -> list[TeamSeasonScope]:
    team_scopes = list(requested_team_seasons)
    seen_scope_keys = {(scope.team.team_id, scope.season.id) for scope in team_scopes}
    for game in games:
        scope = TeamSeasonScope(team=game.opponent_team, season=game.season)
        scope_key = (scope.team.team_id, scope.season.id)
        if scope_key in seen_scope_keys:
            continue
        team_scopes.append(scope)
        seen_scope_keys.add(scope_key)
    return team_scopes


def _filter_rawr_scope(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    *,
    teams: list[Team],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    team_ids = {team.team_id for team in teams}
    selected_game_ids = {game.game_id for game in games if game.team.team_id in team_ids}
    if not selected_game_ids:
        raise ValueError("No games matched the requested RAWR scope")
    filtered_games = [game for game in games if game.game_id in selected_game_ids]
    filtered_game_players = [
        player for player in game_players if player.game_id in selected_game_ids
    ]
    return filtered_games, filtered_game_players


def _exclude_rawr_games_without_positive_minutes(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    positive_minutes_by_game_team: dict[tuple[str, int], bool] = {}
    for player in game_players:
        if not player.has_positive_minutes():
            continue
        positive_minutes_by_game_team[(player.game_id, player.team.team_id)] = True

    valid_game_ids: set[str] = set()
    for game in games:
        if not positive_minutes_by_game_team.get((game.game_id, game.team.team_id), False):
            continue
        if not positive_minutes_by_game_team.get((game.game_id, game.opponent_team.team_id), False):
            continue
        valid_game_ids.add(game.game_id)

    filtered_games = [game for game in games if game.game_id in valid_game_ids]
    filtered_game_players = [player for player in game_players if player.game_id in valid_game_ids]
    return filtered_games, filtered_game_players
