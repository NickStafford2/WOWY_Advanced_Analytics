from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from time import perf_counter

from rawr_analytics.data.game_cache.store import list_cached_scopes, load_team_season_cache
from rawr_analytics.metrics.rawr.cache_status import list_complete_rawr_seasons
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, normalize_seasons
from rawr_analytics.shared.team import Team

type RawrSeasonProgressFn = Callable[[int, int, Season], None]


def load_rawr_input_records(
    *,
    teams: list[Team],
    seasons: list[Season],
    progress_fn: RawrSeasonProgressFn | None = None,
) -> tuple[
    dict[Season, list[NormalizedGameRecord]],
    dict[Season, list[NormalizedGamePlayerRecord]],
]:
    print("load_rawr_input_records()")
    assert seasons, "RAWR record loading requires a non-empty season list"
    assert teams, "RAWR record loading requires a non-empty team list"
    requested_team_seasons = _build_requested_rawr_team_seasons(
        teams=teams,
        seasons=seasons,
    )
    if not requested_team_seasons:
        raise ValueError("No cached data matched the requested RAWR scope")

    teams_by_season = _build_teams_by_season(requested_team_seasons)
    complete_seasons = list_complete_rawr_seasons(seasons=list(teams_by_season))
    if not complete_seasons:
        raise ValueError("No complete cached seasons matched the requested RAWR scope")

    print("load_rawr_input_records() 2")
    season_games: dict[Season, list[NormalizedGameRecord]] = {}
    season_game_players: dict[Season, list[NormalizedGamePlayerRecord]] = {}
    sorted_seasons = normalize_seasons(list(teams_by_season))
    assert sorted_seasons is not None, "RAWR team grouping produced no seasons"
    total_seasons = len(sorted_seasons)
    print("load_rawr_input_records() 3")
    for season_index, season in enumerate(sorted_seasons, start=1):
        print(f"load_rawr_input_records() loop {season.id}")
        if progress_fn is not None:
            # todo. make this function rigorous. it gets stuck at 92% 1/3 of the way through
            progress_fn(season_index, total_seasons, season)
        if season not in complete_seasons:
            print(f"load_rawr_input_records() loop {season.id} is incomplete")
            continue
        season_records = _load_rawr_season_records(
            teams=teams_by_season[season],
            season=season,
        )
        if season_records is None:
            print(f"load_rawr_input_records() loop {season.id} season_records is None")
            continue
        games, game_players = season_records
        season_games[season] = games
        season_game_players[season] = game_players
    print("load_rawr_input_records() end")
    return season_games, season_game_players


def _build_teams_by_season(
    team_seasons: list[TeamSeasonScope],
) -> dict[Season, list[Team]]:
    teams_by_season: dict[Season, list[Team]] = defaultdict(list)
    for scope in team_seasons:
        teams_by_season[scope.season].append(scope.team)
    return teams_by_season


def _build_requested_rawr_team_seasons(
    *,
    teams: list[Team],
    seasons: list[Season],
) -> list[TeamSeasonScope]:
    cached_scopes_by_key = {
        (scope.team.team_id, scope.season): scope
        for scope in list_cached_scopes(teams=teams, seasons=seasons)
    }
    team_seasons: list[TeamSeasonScope] = []
    for season in seasons:
        for team in teams:
            if not team.is_active_during(season):
                continue
            cached_scope = cached_scopes_by_key.get((team.team_id, season))
            if cached_scope is None:
                continue
            team_seasons.append(cached_scope)
    return team_seasons


def _load_rawr_season_records(
    *,
    teams: list[Team],
    season: Season,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]] | None:
    total_start = perf_counter()
    print(f"\n_load_rawr_season_records season={season.id} teams={len(teams)}")

    step_start = perf_counter()
    requested_team_seasons = _build_requested_rawr_team_seasons(
        teams=teams,
        seasons=[season],
    )
    print(
        f"  _build_requested_rawr_team_seasons: "
        f"{perf_counter() - step_start:.4f}s "
        f"(requested_team_seasons={len(requested_team_seasons)})"
    )

    if not requested_team_seasons:
        print("  requested_team_seasons is empty")
        print(f"  total: {perf_counter() - total_start:.4f}s")
        return None

    step_start = perf_counter()
    requested_games, _ = load_team_season_cache(requested_team_seasons)
    print(
        f"  load_team_season_cache(requested_team_seasons): "
        f"{perf_counter() - step_start:.4f}s "
        f"(requested_games={len(requested_games)})"
    )

    step_start = perf_counter()
    season_scopes = _expand_rawr_season_scopes(
        requested_team_seasons=requested_team_seasons,
        games=requested_games,
    )
    print(
        f"  _expand_rawr_season_scopes: "
        f"{perf_counter() - step_start:.4f}s "
        f"(season_scopes={len(season_scopes)})"
    )

    assert {(s.team.team_id, s.season.id) for s in requested_team_seasons}
        == {(s.team.team_id, s.season.id) for s in season_scopes}, "Scopes are not the same:",
    step_start = perf_counter()
    games, game_players = load_team_season_cache(season_scopes)
    print(
        f"  load_team_season_cache(season_scopes): "
        f"{perf_counter() - step_start:.4f}s "
        f"(games={len(games)}, game_players={len(game_players)})"
    )

    step_start = perf_counter()
    requested_teams = [scope.team for scope in requested_team_seasons]
    games, game_players = _filter_rawr_scope(
        games,
        game_players,
        teams=requested_teams,
    )
    print(
        f"  _filter_rawr_scope: "
        f"{perf_counter() - step_start:.4f}s "
        f"(games={len(games)}, game_players={len(game_players)})"
    )

    step_start = perf_counter()
    games, game_players = _exclude_rawr_games_without_positive_minutes(
        games,
        game_players,
    )
    print(
        f"  _exclude_rawr_games_without_positive_minutes: "
        f"{perf_counter() - step_start:.4f}s "
        f"(games={len(games)}, game_players={len(game_players)})"
    )

    print(f"  total: {perf_counter() - total_start:.4f}s")

    if not games:
        print("  no games after filtering")
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
