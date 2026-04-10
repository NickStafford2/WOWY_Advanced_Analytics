from __future__ import annotations

import sqlite3
from collections.abc import Sequence

from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


def replace_team_season_cache_rows(
    connection: sqlite3.Connection,
    *,
    scope: TeamSeasonScope,
    games: Sequence[NormalizedGameRecord],
    game_players: Sequence[NormalizedGamePlayerRecord],
    source_path: str,
    source_snapshot: str,
    source_kind: str,
    build_version: str,
    refreshed_at: str,
    expected_games_count: int | None,
    skipped_games_count: int | None,
) -> None:
    season_type = scope.season.season_type.to_nba_format()
    connection.execute(
        """
        DELETE FROM normalized_game_players
        WHERE team_id = ? AND season = ? AND season_type = ?
        """,
        (scope.team.team_id, scope.season.year_string_nba_api, season_type),
    )
    connection.execute(
        """
        DELETE FROM normalized_games
        WHERE team_id = ? AND season = ? AND season_type = ?
        """,
        (scope.team.team_id, scope.season.year_string_nba_api, season_type),
    )
    connection.executemany(
        """
        INSERT INTO normalized_games (
            game_id,
            season,
            game_date,
            team_id,
            opponent_team_id,
            is_home,
            margin,
            season_type,
            source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                game.game_id,
                game.season.year_string_nba_api,
                game.game_date,
                game.team.team_id,
                game.opponent_team.team_id,
                int(game.is_home),
                game.margin,
                game.season.season_type.to_nba_format(),
                game.source,
            )
            for game in games
        ],
    )
    connection.executemany(
        """
        INSERT INTO normalized_game_players (
            game_id,
            season,
            season_type,
            team_id,
            player_id,
            player_name,
            appeared,
            minutes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                game_player.game_id,
                scope.season.year_string_nba_api,
                season_type,
                game_player.team.team_id,
                game_player.player.player_id,
                game_player.player.player_name,
                int(game_player.appeared),
                game_player.minutes,
            )
            for game_player in game_players
        ],
    )
    connection.execute(
        """
        INSERT INTO normalized_cache_loads (
            team_id,
            season,
            season_type,
            source_path,
            source_snapshot,
            source_kind,
            build_version,
            refreshed_at,
            games_row_count,
            game_players_row_count,
            expected_games_row_count,
            skipped_games_row_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(team_id, season, season_type) DO UPDATE SET
            source_path = excluded.source_path,
            source_snapshot = excluded.source_snapshot,
            source_kind = excluded.source_kind,
            build_version = excluded.build_version,
            refreshed_at = excluded.refreshed_at,
            games_row_count = excluded.games_row_count,
            game_players_row_count = excluded.game_players_row_count,
            expected_games_row_count = excluded.expected_games_row_count,
            skipped_games_row_count = excluded.skipped_games_row_count
        """,
        (
            scope.team.team_id,
            scope.season.year_string_nba_api,
            season_type,
            source_path,
            source_snapshot,
            source_kind,
            build_version,
            refreshed_at,
            len(games),
            len(game_players),
            expected_games_count,
            skipped_games_count,
        ),
    )


def select_normalized_game_rows(
    connection: sqlite3.Connection,
    *,
    team_seasons: Sequence[TeamSeasonScope] | None = None,
    teams: Sequence[Team] | None = None,
    seasons: Sequence[Season] | None = None,
    game_ids: Sequence[str] | None = None,
) -> list[sqlite3.Row]:
    query = """
        SELECT
            game_id,
            season,
            game_date,
            team_id,
            opponent_team_id,
            is_home,
            margin,
            season_type,
            source
        FROM normalized_games
        WHERE 1 = 1
    """
    params: list[object] = []
    query, params = _append_team_season_filter(
        query,
        params,
        team_seasons=team_seasons,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="team_id",
        values=[team.team_id for team in teams or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="season",
        values=[season.year_string_nba_api for season in seasons or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="season_type",
        values=sorted({season.season_type.to_nba_format() for season in seasons or []}),
    )
    query, params = _append_in_filter(query, params, column="game_id", values=list(game_ids or []))
    query += " ORDER BY season, game_date, team_id, game_id"
    return connection.execute(query, params).fetchall()


def select_normalized_game_player_rows(
    connection: sqlite3.Connection,
    *,
    team_seasons: Sequence[TeamSeasonScope] | None = None,
    teams: Sequence[Team] | None = None,
    seasons: Sequence[Season] | None = None,
    game_ids: Sequence[str] | None = None,
) -> list[sqlite3.Row]:
    query = """
        SELECT
            game_id,
            season,
            season_type,
            team_id,
            player_id,
            player_name,
            appeared,
            minutes
        FROM normalized_game_players
        WHERE 1 = 1
    """
    params: list[object] = []
    query, params = _append_team_season_filter(
        query,
        params,
        team_seasons=team_seasons,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="team_id",
        values=[team.team_id for team in teams or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="season",
        values=[season.year_string_nba_api for season in seasons or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="season_type",
        values=sorted({season.season_type.to_nba_format() for season in seasons or []}),
    )
    query, params = _append_in_filter(query, params, column="game_id", values=list(game_ids or []))
    query += " ORDER BY season, team_id, game_id, player_id"
    return connection.execute(query, params).fetchall()


def select_cache_load_rows(
    connection: sqlite3.Connection,
    *,
    seasons: Sequence[Season] | None = None,
    teams: Sequence[Team] | None = None,
) -> list[sqlite3.Row]:
    query = """
        SELECT
            team_id,
            season,
            season_type,
            source_path,
            source_snapshot,
            source_kind,
            build_version,
            refreshed_at,
            games_row_count,
            game_players_row_count,
            expected_games_row_count,
            skipped_games_row_count
        FROM normalized_cache_loads
        WHERE 1 = 1
    """
    params: list[object] = []
    query, params = _append_in_filter(
        query,
        params,
        column="team_id",
        values=[team.team_id for team in teams or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="season",
        values=[season.year_string_nba_api for season in seasons or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="season_type",
        values=[season.season_type.value for season in seasons or []],
    )
    query += " ORDER BY season, season_type, team_id"
    return connection.execute(query, params).fetchall()


def _append_team_season_filter(
    query: str,
    params: list[object],
    *,
    team_seasons: Sequence[TeamSeasonScope] | None,
) -> tuple[str, list[object]]:
    if not team_seasons:
        return query, params
    scope_keys = sorted(
        {
            (
                scope.team.team_id,
                scope.season.year_string_nba_api,
                scope.season.season_type.to_nba_format(),
            )
            for scope in team_seasons
        },
        key=lambda item: (item[1], item[2], item[0]),
    )
    query += " AND ("
    query += " OR ".join("(team_id = ? AND season = ? AND season_type = ?)" for _ in scope_keys)
    query += ")"
    for team_id, season_id, season_type in scope_keys:
        params.extend((team_id, season_id, season_type))
    return query, params


def _append_in_filter(
    query: str,
    params: list[object],
    *,
    column: str,
    values: Sequence[object],
) -> tuple[str, list[object]]:
    if not values:
        return query, params
    query += f" AND {column} IN ({','.join('?' for _ in values)})"
    params.extend(values)
    return query, params
