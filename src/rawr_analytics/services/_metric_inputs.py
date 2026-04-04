from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.game_cache import (
    list_cache_load_rows,
    load_normalized_scope_records_from_db,
)
from rawr_analytics.data.game_cache.rows import NormalizedGamePlayerRow, NormalizedGameRow
from rawr_analytics.data.scope_resolver import resolve_team_seasons
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

type RawrSeasonProgressFn = Callable[[int, int, Season], None]


@dataclass
class _SeasonCacheSummary:
    team_labels: set[str]
    incomplete_metadata_teams: set[str]
    partial_teams: dict[str, tuple[int, int]]
    skipped_teams: dict[str, int]


def load_rawr_records(
    *,
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType,
    progress_fn: RawrSeasonProgressFn | None = None,
) -> tuple[
    dict[Season, list[NormalizedGameRecord]],
    dict[Season, list[NormalizedGamePlayerRecord]],
]:
    requested_team_seasons = resolve_team_seasons(teams, seasons, season_type=season_type)
    if not requested_team_seasons:
        raise ValueError("No cached data matched the requested RAWR scope")
    teams_by_season: dict[Season, list[Team]] = defaultdict(list)
    for scope in requested_team_seasons:
        teams_by_season[scope.season].append(scope.team)

    complete_seasons = set(
        _select_complete_rawr_scope_seasons(
            teams=teams,
            seasons=seasons,
            season_type=season_type,
        )
    )
    if not complete_seasons:
        raise ValueError("No complete cached seasons matched the requested RAWR scope")
    season_games: dict[Season, list[NormalizedGameRecord]] = {}
    season_game_players: dict[Season, list[NormalizedGamePlayerRecord]] = {}
    sorted_seasons = sorted(teams_by_season, key=lambda item: item.id)
    total_seasons = len(sorted_seasons)
    for season_index, season in enumerate(sorted_seasons, start=1):
        if progress_fn is not None:
            progress_fn(season_index, total_seasons, season)
        if season.id not in complete_seasons:
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


def load_wowy_records(
    *,
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    team_seasons = resolve_team_seasons(teams, seasons, season_type=season_type)
    if not team_seasons:
        raise ValueError("No cached data matched the requested scope")

    game_rows, game_player_rows = load_normalized_scope_records_from_db(team_seasons)
    games = [_build_normalized_game_record(row) for row in game_rows]
    game_players = [_build_normalized_game_player_record(row) for row in game_player_rows]
    return games, game_players


def list_incomplete_rawr_season_warnings(
    *,
    seasons: list[str] | None = None,
    season_type: SeasonType,
) -> list[str]:
    requested_seasons = seasons or _list_cached_rawr_seasons_for_type(season_type)
    summaries = _summarize_rawr_cache_seasons(seasons=requested_seasons, season_type=season_type)
    warnings: list[str] = []
    for season in requested_seasons:
        summary = summaries.get(season)
        if summary is None:
            warnings.append(f"{season}: no cache load metadata found")
            continue
        warnings.extend(_build_rawr_warning_messages(season=season, summary=summary))
    return warnings


def _load_rawr_season_records(
    *,
    teams: list[Team],
    season: Season,
    season_type: SeasonType,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]] | None:
    requested_team_seasons = resolve_team_seasons(teams, [season], season_type=season_type)
    if not requested_team_seasons:
        return None

    game_rows, _ = load_normalized_scope_records_from_db(requested_team_seasons)
    games = [_build_normalized_game_record(row) for row in game_rows]
    team_scopes = list(requested_team_seasons)
    seen_scope_keys = {
        (
            scope.team.team_id,
            scope.season.id,
            scope.season.season_type.value,
        )
        for scope in team_scopes
    }
    for game in games:
        scope = TeamSeasonScope(team=game.opponent_team, season=game.season)
        scope_key = (scope.team.team_id, scope.season.id, scope.season.season_type.value)
        if scope_key in seen_scope_keys:
            continue
        team_scopes.append(scope)
        seen_scope_keys.add(scope_key)
    game_rows, game_player_rows = load_normalized_scope_records_from_db(team_scopes)
    games = [_build_normalized_game_record(row) for row in game_rows]
    game_players = [_build_normalized_game_player_record(row) for row in game_player_rows]
    games, game_players = _filter_rawr_scope(
        games,
        game_players,
        teams=[scope.team for scope in requested_team_seasons],
        seasons=[season],
    )
    games, game_players = _exclude_rawr_games_without_positive_minutes(games, game_players)
    if not games:
        return None
    return games, game_players


def _filter_rawr_scope(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    teams: list[Team] | None,
    seasons: list[Season] | None,
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    if not teams and not seasons:
        return games, game_players
    normalized_team_ids = {team.team_id for team in teams or []}
    normalized_seasons = set(seasons or [])
    selected_game_ids = {
        game.game_id
        for game in games
        if (not normalized_seasons or game.season in normalized_seasons)
        and (not normalized_team_ids or game.team.team_id in normalized_team_ids)
    }
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


def _build_normalized_game_record(row: NormalizedGameRow) -> NormalizedGameRecord:
    return NormalizedGameRecord(
        game_id=row.game_id,
        game_date=row.game_date,
        season=row.season,
        team=row.team,
        opponent_team=row.opponent_team,
        is_home=row.is_home,
        margin=row.margin,
        source=row.source,
    )


def _build_normalized_game_player_record(
    row: NormalizedGamePlayerRow,
) -> NormalizedGamePlayerRecord:
    return NormalizedGamePlayerRecord(
        game_id=row.game_id,
        player=row.player,
        appeared=row.appeared,
        minutes=row.minutes,
        team=row.team,
    )


def _list_expected_rawr_teams_for_season(season: str) -> list[str]:
    resolved_season = Season.parse(season, "Regular Season")
    return [
        team.abbreviation(season=resolved_season)
        for team in Team.all_active_in_season(resolved_season)
    ]


def _list_complete_rawr_seasons(
    *,
    seasons: list[str],
    season_type: SeasonType,
) -> set[str]:
    summaries = _summarize_rawr_cache_seasons(seasons=seasons, season_type=season_type)
    return {
        season
        for season in seasons
        if season in summaries
        and _is_complete_rawr_summary(season=season, summary=summaries[season])
    }


def _select_complete_rawr_scope_seasons(
    *,
    teams: list[Team] | None,
    seasons: list[Season] | None,
    season_type: SeasonType,
) -> list[str]:
    team_seasons = resolve_team_seasons(teams, seasons, season_type=season_type)
    candidate_seasons = sorted({team_season.season.id for team_season in team_seasons})
    if not candidate_seasons:
        return []
    complete_seasons = _list_complete_rawr_seasons(
        seasons=candidate_seasons,
        season_type=season_type,
    )
    return [season for season in candidate_seasons if season in complete_seasons]


def _summarize_rawr_cache_seasons(
    *,
    seasons: list[str],
    season_type: SeasonType,
) -> dict[str, _SeasonCacheSummary]:
    season_filter = set(seasons)
    summaries: dict[str, _SeasonCacheSummary] = {}
    for row in list_cache_load_rows():
        if row.season.id not in season_filter or row.season.season_type != season_type:
            continue
        summary = summaries.setdefault(
            row.season.id,
            _SeasonCacheSummary(
                team_labels=set(),
                incomplete_metadata_teams=set(),
                partial_teams={},
                skipped_teams={},
            ),
        )
        team_label = row.team.abbreviation(season=row.season)
        summary.team_labels.add(team_label)
        if row.expected_games_row_count is None or row.skipped_games_row_count is None:
            summary.incomplete_metadata_teams.add(team_label)
            continue
        if row.games_row_count != row.expected_games_row_count:
            summary.partial_teams[team_label] = (row.games_row_count, row.expected_games_row_count)
        if row.skipped_games_row_count != 0:
            summary.skipped_teams[team_label] = row.skipped_games_row_count
    return summaries


def _is_complete_rawr_summary(*, season: str, summary: _SeasonCacheSummary) -> bool:
    return (
        summary.team_labels == set(_list_expected_rawr_teams_for_season(season))
        and not summary.incomplete_metadata_teams
        and not summary.partial_teams
        and not summary.skipped_teams
    )


def _build_rawr_warning_messages(*, season: str, summary: _SeasonCacheSummary) -> list[str]:
    warnings: list[str] = []
    missing_teams = sorted(set(_list_expected_rawr_teams_for_season(season)) - summary.team_labels)
    if missing_teams:
        warnings.append(f"{season}: missing team-seasons: {', '.join(missing_teams)}")
    warnings.extend(
        f"{season}: incomplete cache metadata for {team_label}"
        for team_label in sorted(summary.incomplete_metadata_teams)
    )
    warnings.extend(
        f"{season}: partial team-season cache for {team_label} ({games}/{expected} games)"
        for team_label, (games, expected) in sorted(summary.partial_teams.items())
    )
    warnings.extend(
        f"{season}: skipped games present for {team_label} ({skipped} skipped)"
        for team_label, skipped in sorted(summary.skipped_teams.items())
    )
    return warnings


def _list_cached_rawr_seasons_for_type(season_type: SeasonType) -> list[str]:
    return sorted(
        {row.season.id for row in list_cache_load_rows() if row.season.season_type == season_type}
    )
