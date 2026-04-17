# src/rawr_analytics/metrics/rawr/cache.py

from __future__ import annotations

from collections import defaultdict

from rawr_analytics.data.game_cache.store import (
    list_cached_scopes,
    load_games_for_team_seasons_with_opponents,
)
from rawr_analytics.metrics.rawr.cache_status import list_complete_rawr_seasons
from rawr_analytics.metrics.rawr.progress import (
    RawrProgressSink,
    emit_rawr_progress,
    emit_rawr_season_progress,
)
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, normalize_seasons
from rawr_analytics.shared.team import Team


def load_rawr_input_records(
    *,
    teams: list[Team],
    seasons: list[Season],
    progress_sink: RawrProgressSink | None = None,
) -> tuple[
    dict[Season, list[NormalizedGameRecord]],
    dict[Season, list[NormalizedGamePlayerRecord]],
]:
    print("load_rawr_input_records()")
    assert seasons, "RAWR record loading requires a non-empty season list"
    assert teams, "RAWR record loading requires a non-empty team list"

    emit_rawr_progress(
        progress_sink,
        phase="resolve",
        current=0,
        total=1,
        detail="Matching query filters to cached team-season scope.",
    )
    requested_team_seasons = _build_requested_rawr_team_seasons(
        teams=teams,
        seasons=seasons,
    )
    emit_rawr_progress(
        progress_sink,
        phase="resolve",
        current=1,
        total=1,
        detail=f"Matched {len(requested_team_seasons)} cached team-seasons.",
    )
    if not requested_team_seasons:
        raise ValueError("No cached data matched the requested RAWR scope")

    requested_team_seasons_by_season = _group_team_seasons_by_season(requested_team_seasons)
    complete_seasons = list_complete_rawr_seasons(seasons=list(requested_team_seasons_by_season))
    if not complete_seasons:
        raise ValueError("No complete cached seasons matched the requested RAWR scope")

    complete_team_seasons = _collect_complete_team_seasons(
        requested_team_seasons_by_season=requested_team_seasons_by_season,
        complete_seasons=complete_seasons,
    )
    if not complete_team_seasons:
        raise ValueError("No complete cached team-seasons matched the requested RAWR scope")

    print(
        "load_rawr_input_records() bulk loading "
        f"{len(complete_team_seasons)} team-seasons across {len(complete_seasons)} seasons"
    )
    emit_rawr_progress(
        progress_sink,
        phase="db-load",
        current=0,
        total=1,
        detail=(
            f"Loading normalized rows for {len(complete_team_seasons)} complete team-seasons "
            f"across {len(complete_seasons)} seasons."
        ),
    )
    all_games, all_game_players = _load_rawr_records_for_complete_seasons(
        requested_team_seasons=complete_team_seasons,
    )
    emit_rawr_progress(
        progress_sink,
        phase="db-load",
        current=1,
        total=1,
        detail=f"Loaded {len(all_games)} game rows and {len(all_game_players)} player rows.",
    )

    emit_rawr_progress(
        progress_sink,
        phase="grouping",
        current=0,
        total=1,
        detail="Grouping loaded rows by season.",
    )
    games_by_season, game_players_by_season = _group_loaded_rawr_records_by_season(
        all_games,
        all_game_players,
    )
    emit_rawr_progress(
        progress_sink,
        phase="grouping",
        current=1,
        total=1,
        detail=f"Grouped rows into {len(games_by_season)} seasons.",
    )

    season_games: dict[Season, list[NormalizedGameRecord]] = {}
    season_game_players: dict[Season, list[NormalizedGamePlayerRecord]] = {}
    sorted_seasons = normalize_seasons(list(requested_team_seasons_by_season))
    assert sorted_seasons is not None, "RAWR team grouping produced no seasons"

    total_seasons = len(sorted_seasons)
    for season_index, season in enumerate(sorted_seasons, start=1):
        print(f"load_rawr_input_records() loop {season.id}")
        emit_rawr_season_progress(
            progress_sink,
            phase="season-filter",
            current=season_index - 1,
            total=total_seasons,
            season=season,
        )

        if season not in complete_seasons:
            print(f"load_rawr_input_records() loop {season.id} is incomplete")
            emit_rawr_season_progress(
                progress_sink,
                phase="season-filter",
                current=season_index,
                total=total_seasons,
                season=season,
            )
            continue

        season_records = _filter_grouped_rawr_season_records(
            season=season,
            requested_team_ids={
                scope.team.team_id for scope in requested_team_seasons_by_season[season]
            },
            games=games_by_season.get(season, []),
            game_players=game_players_by_season.get(season, []),
        )
        if season_records is None:
            print(f"load_rawr_input_records() loop {season.id} season_records is None")
            emit_rawr_season_progress(
                progress_sink,
                phase="season-filter",
                current=season_index,
                total=total_seasons,
                season=season,
            )
            continue

        games, game_players = season_records
        season_games[season] = games
        season_game_players[season] = game_players
        emit_rawr_season_progress(
            progress_sink,
            phase="season-filter",
            current=season_index,
            total=total_seasons,
            season=season,
        )

    print("load_rawr_input_records() end")
    return season_games, season_game_players


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


def _group_team_seasons_by_season(
    team_seasons: list[TeamSeasonScope],
) -> dict[Season, list[TeamSeasonScope]]:
    grouped: dict[Season, list[TeamSeasonScope]] = defaultdict(list)
    for scope in team_seasons:
        grouped[scope.season].append(scope)
    return dict(grouped)


def _collect_complete_team_seasons(
    *,
    requested_team_seasons_by_season: dict[Season, list[TeamSeasonScope]],
    complete_seasons: set[Season],
) -> list[TeamSeasonScope]:
    complete_team_seasons: list[TeamSeasonScope] = []
    sorted_seasons = normalize_seasons(list(requested_team_seasons_by_season))
    assert sorted_seasons is not None, "RAWR team grouping produced no seasons"

    for season in sorted_seasons:
        if season not in complete_seasons:
            continue
        complete_team_seasons.extend(requested_team_seasons_by_season[season])

    return complete_team_seasons


def _load_rawr_records_for_complete_seasons(
    *,
    requested_team_seasons: list[TeamSeasonScope],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    print(
        "_load_rawr_records_for_complete_seasons() "
        f"{len(requested_team_seasons)} requested_team_seasons"
    )
    assert requested_team_seasons, "RAWR bulk season loading requires team-season scopes"

    games, game_players = load_games_for_team_seasons_with_opponents(
        requested_team_seasons,
        validate_cached_scopes=False,
    )

    print(
        "_load_rawr_records_for_complete_seasons() "
        f"loaded {len(games)} games and {len(game_players)} game_players"
    )
    return games, game_players


def _group_loaded_rawr_records_by_season(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> tuple[
    dict[Season, list[NormalizedGameRecord]],
    dict[Season, list[NormalizedGamePlayerRecord]],
]:
    print(
        "_group_loaded_rawr_records_by_season() "
        f"{len(games)} games {len(game_players)} game_players"
    )

    games_by_season: dict[Season, list[NormalizedGameRecord]] = defaultdict(list)
    season_by_game_id: dict[str, Season] = {}

    for game in games:
        games_by_season[game.season].append(game)
        season_by_game_id[game.game_id] = game.season

    game_players_by_season: dict[Season, list[NormalizedGamePlayerRecord]] = defaultdict(list)
    for player in game_players:
        season = season_by_game_id.get(player.game_id)
        if season is None:
            continue
        game_players_by_season[season].append(player)

    print(
        "_group_loaded_rawr_records_by_season() grouped into "
        f"{len(games_by_season)} game seasons and {len(game_players_by_season)} player seasons"
    )
    return dict(games_by_season), dict(game_players_by_season)


def _filter_grouped_rawr_season_records(
    *,
    season: Season,
    requested_team_ids: set[int],
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]] | None:
    print(
        "_filter_grouped_rawr_season_records() "
        f"{season.id} {len(games)} games {len(game_players)} game_players"
    )
    if not requested_team_ids:
        return None
    if not games or not game_players:
        return None

    games, game_players = _filter_rawr_loaded_records(
        games,
        game_players,
        requested_team_ids=requested_team_ids,
    )

    print(
        "_filter_grouped_rawr_season_records() "
        f"{season.id} filtered to {len(games)} games {len(game_players)} game_players"
    )
    if not games or not game_players:
        return None
    return games, game_players


def _filter_rawr_loaded_records(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    *,
    requested_team_ids: set[int],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    positive_minute_team_ids_by_game_id: dict[str, set[int]] = defaultdict(set)

    for player in game_players:
        if not player.has_positive_minutes():
            continue
        positive_minute_team_ids_by_game_id[player.game_id].add(player.team.team_id)

    valid_game_ids: set[str] = set()

    for game in games:
        if game.team.team_id not in requested_team_ids:
            continue

        team_ids_with_minutes = positive_minute_team_ids_by_game_id.get(game.game_id)
        if team_ids_with_minutes is None:
            continue

        if game.team.team_id not in team_ids_with_minutes:
            continue
        if game.opponent_team.team_id not in team_ids_with_minutes:
            continue

        valid_game_ids.add(game.game_id)

    filtered_games = [game for game in games if game.game_id in valid_game_ids]
    filtered_game_players = [player for player in game_players if player.game_id in valid_game_ids]

    return filtered_games, filtered_game_players
