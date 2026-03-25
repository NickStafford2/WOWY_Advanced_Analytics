from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from wowy.data.game_cache.rows import NormalizedCacheLoadRow
from wowy.data.game_cache.schema import _connect, initialize_game_cache_db
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.normalize.validation import validate_normalized_cache_batch
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.team_identity import (
    canonical_team_lookup_abbreviation,
    resolve_team_id,
    resolve_team_identity_from_id_and_season,
)
from wowy.nba.team_seasons import TeamSeasonScope

GAME_CACHE_BUILD_VERSION = "normalized-cache-v2"
REGULAR_SEASON = "Regular Season"


def replace_team_season_normalized_rows(
    db_path: Path,
    *,
    team: str,
    team_id: int,
    season: str,
    season_type: str,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    source_path: str,
    source_snapshot: str,
    source_kind: str,
    build_version: str = GAME_CACHE_BUILD_VERSION,
    expected_games_row_count: int | None = None,
    skipped_games_row_count: int | None = None,
) -> None:
    initialize_game_cache_db(db_path)
    team = team.upper()
    season = canonicalize_season_string(season)
    if team_id <= 0:
        raise ValueError(f"team_id must be positive for normalized cache writes: {team_id!r}")
    canonical_team = resolve_team_identity_from_id_and_season(team_id, season).abbreviation
    season_type = canonicalize_season_type(season_type)
    validate_normalized_cache_batch(
        team=canonical_team,
        team_id=team_id,
        season=season,
        season_type=season_type,
        games=games,
        game_players=game_players,
    )
    refreshed_at = datetime.now(UTC).isoformat()

    with _connect(db_path) as connection:
        connection.execute("BEGIN")
        _upsert_team_history_for_scope(
            connection,
            team_id=team_id,
            team=canonical_team,
            season=season,
        )
        _upsert_team_history_for_games(connection, games)
        connection.execute(
            """
            DELETE FROM normalized_game_players
            WHERE team_id = ? AND season = ? AND season_type = ?
            """,
            (team_id, season, season_type),
        )
        connection.execute(
            """
            DELETE FROM normalized_games
            WHERE team_id = ? AND season = ? AND season_type = ?
            """,
            (team_id, season, season_type),
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
                    game.season,
                    game.game_date,
                    game.team_id,
                    game.opponent_team_id,
                    int(game.is_home),
                    game.margin,
                    game.season_type,
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
                    player.game_id,
                    season,
                    season_type,
                    player.team_id,
                    player.player_id,
                    player.player_name,
                    int(player.appeared),
                    player.minutes,
                )
                for player in game_players
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
                team_id = excluded.team_id,
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
                team_id,
                season,
                season_type,
                source_path,
                source_snapshot,
                source_kind,
                build_version,
                refreshed_at,
                len(games),
                len(game_players),
                expected_games_row_count,
                skipped_games_row_count,
            ),
        )
        connection.commit()


def load_normalized_games_from_db(
    db_path: Path,
    *,
    season_type: str,
    teams: list[str] | None = None,
    seasons: list[str] | None = None,
    game_ids: list[str] | None = None,
) -> list[NormalizedGameRecord]:
    initialize_game_cache_db(db_path)
    season_type = canonicalize_season_type(season_type)
    normalized_seasons = [canonicalize_season_string(season) for season in seasons or []]
    query = """
        SELECT
            game.game_id,
            game.season,
            game.game_date,
            team_history.abbreviation AS team,
            game.team_id,
            opponent_history.abbreviation AS opponent,
            game.opponent_team_id,
            game.is_home,
            game.margin,
            game.season_type,
            game.source
        FROM normalized_games AS game
        JOIN team_history
          ON team_history.team_id = game.team_id
         AND team_history.season = game.season
        JOIN team_history AS opponent_history
          ON opponent_history.team_id = game.opponent_team_id
         AND opponent_history.season = game.season
        WHERE game.season_type = ?
    """
    params: list[object] = [season_type]
    query, params = _append_in_filter(
        query,
        params,
        column="game.team_id",
        values=_resolve_team_ids(teams, seasons=normalized_seasons),
    )
    query, params = _append_in_filter(
        query,
        params,
        column="game.season",
        values=normalized_seasons,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="game.game_id",
        values=game_ids or [],
    )
    query += " ORDER BY game.season, game.game_date, team_history.abbreviation, game.game_id"

    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        NormalizedGameRecord(
            game_id=row["game_id"],
            season=row["season"],
            game_date=row["game_date"],
            team=row["team"],
            team_id=row["team_id"],
            opponent=row["opponent"],
            opponent_team_id=row["opponent_team_id"],
            is_home=bool(row["is_home"]),
            margin=row["margin"],
            season_type=row["season_type"],
            source=row["source"],
        )
        for row in rows
    ]


def load_normalized_game_players_from_db(
    db_path: Path,
    *,
    season_type: str,
    teams: list[str] | None = None,
    seasons: list[str] | None = None,
    game_ids: list[str] | None = None,
) -> list[NormalizedGamePlayerRecord]:
    initialize_game_cache_db(db_path)
    season_type = canonicalize_season_type(season_type)
    normalized_seasons = [canonicalize_season_string(season) for season in seasons or []]
    query = """
        SELECT
            player.game_id,
            team_history.abbreviation AS team,
            player.team_id,
            player.player_id,
            player.player_name,
            player.appeared,
            player.minutes
        FROM normalized_game_players AS player
        JOIN team_history
          ON team_history.team_id = player.team_id
         AND team_history.season = player.season
        WHERE player.season_type = ?
    """
    params: list[object] = [season_type]
    query, params = _append_in_filter(
        query,
        params,
        column="player.team_id",
        values=_resolve_team_ids(teams, seasons=normalized_seasons),
    )
    query, params = _append_in_filter(
        query,
        params,
        column="player.season",
        values=normalized_seasons,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="player.game_id",
        values=game_ids or [],
    )
    query += " ORDER BY player.season, team_history.abbreviation, player.game_id, player.player_id"

    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        NormalizedGamePlayerRecord(
            game_id=row["game_id"],
            team=row["team"],
            team_id=row["team_id"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            appeared=bool(row["appeared"]),
            minutes=row["minutes"],
        )
        for row in rows
    ]


def load_cache_load_row(
    db_path: Path,
    *,
    team: str,
    season: str,
    season_type: str,
) -> NormalizedCacheLoadRow | None:
    initialize_game_cache_db(db_path)
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    team_id = resolve_team_id(team, season=season)
    with _connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                team_history.abbreviation AS team,
                load.team_id,
                load.season,
                load.season_type,
                load.source_path,
                load.source_snapshot,
                load.source_kind,
                load.build_version,
                load.refreshed_at,
                load.games_row_count,
                load.game_players_row_count,
                load.expected_games_row_count,
                load.skipped_games_row_count
            FROM normalized_cache_loads AS load
            JOIN team_history
              ON team_history.team_id = load.team_id
             AND team_history.season = load.season
            WHERE load.team_id = ? AND load.season = ? AND load.season_type = ?
            """,
            (team_id, season, season_type),
        ).fetchone()
    if row is None:
        return None
    return NormalizedCacheLoadRow(
        team=row["team"],
        team_id=row["team_id"],
        season=row["season"],
        season_type=row["season_type"],
        source_path=row["source_path"],
        source_snapshot=row["source_snapshot"],
        source_kind=row["source_kind"],
        build_version=row["build_version"],
        refreshed_at=row["refreshed_at"],
        games_row_count=row["games_row_count"],
        game_players_row_count=row["game_players_row_count"],
        expected_games_row_count=row["expected_games_row_count"],
        skipped_games_row_count=row["skipped_games_row_count"],
    )


def list_cache_load_rows(
    db_path: Path,
    *,
    season_type: str | None = None,
    seasons: list[str] | None = None,
    teams: list[str] | None = None,
) -> list[NormalizedCacheLoadRow]:
    if not db_path.exists():
        return []
    initialize_game_cache_db(db_path)
    normalized_seasons = [canonicalize_season_string(season) for season in seasons or []]
    query = """
        SELECT
            team_history.abbreviation AS team,
            load.team_id,
            load.season,
            load.season_type,
            load.source_path,
            load.source_snapshot,
            load.source_kind,
            load.build_version,
            load.refreshed_at,
            load.games_row_count,
            load.game_players_row_count,
            load.expected_games_row_count,
            load.skipped_games_row_count
        FROM normalized_cache_loads AS load
        JOIN team_history
          ON team_history.team_id = load.team_id
         AND team_history.season = load.season
        WHERE 1 = 1
    """
    params: list[object] = []
    if season_type is not None:
        query += " AND load.season_type = ?"
        params.append(canonicalize_season_type(season_type))
    query, params = _append_in_filter(
        query,
        params,
        column="load.season",
        values=normalized_seasons,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="load.team_id",
        values=_resolve_team_ids(teams, seasons=normalized_seasons),
    )
    query += " ORDER BY load.season, load.team_id"
    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        NormalizedCacheLoadRow(
            team=row["team"],
            team_id=row["team_id"],
            season=row["season"],
            season_type=row["season_type"],
            source_path=row["source_path"],
            source_snapshot=row["source_snapshot"],
            source_kind=row["source_kind"],
            build_version=row["build_version"],
            refreshed_at=row["refreshed_at"],
            games_row_count=row["games_row_count"],
            game_players_row_count=row["game_players_row_count"],
            expected_games_row_count=row["expected_games_row_count"],
            skipped_games_row_count=row["skipped_games_row_count"],
        )
        for row in rows
    ]


def list_cached_team_seasons_from_db(
    db_path: Path,
    *,
    season_type: str | None = None,
) -> list[TeamSeasonScope]:
    if not db_path.exists():
        return []
    initialize_game_cache_db(db_path)
    if season_type is not None:
        season_type = canonicalize_season_type(season_type)
    query = """
        SELECT DISTINCT team_history.abbreviation AS team, load.team_id, load.season
        FROM normalized_cache_loads AS load
        JOIN team_history
          ON team_history.team_id = load.team_id
         AND team_history.season = load.season
    """
    params: list[object] = []
    if season_type is not None:
        query += " WHERE load.season_type = ?"
        params.append(season_type)
    query += " ORDER BY load.season, load.team_id"
    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        TeamSeasonScope(team=row["team"], team_id=row["team_id"], season=row["season"])
        for row in rows
    ]


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


def _resolve_team_ids(
    teams: list[str] | None,
    *,
    seasons: list[str] | None = None,
) -> list[int]:
    if not teams:
        return []
    if not seasons:
        return [resolve_team_id(team) for team in teams]
    resolved_team_ids: set[int] = set()
    unresolved_teams: list[str] = []

    for team in teams:
        resolved_for_team = False
        for season in seasons:
            try:
                resolved_team_ids.add(resolve_team_id(team, season=season))
            except ValueError:
                continue
            resolved_for_team = True
        if not resolved_for_team:
            unresolved_teams.append(team)

    if unresolved_teams:
        resolve_team_id(unresolved_teams[0], season=seasons[0])
    return sorted(resolved_team_ids)


def _upsert_team_history_for_scope(
    connection: sqlite3.Connection,
    *,
    team_id: int,
    team: str,
    season: str,
) -> None:
    identity = resolve_team_identity_from_id_and_season(team_id, season)
    if team.strip().upper() != identity.abbreviation:
        raise ValueError(
            f"Team history label {team!r} does not match team_id {team_id!r} "
            f"for season {season!r}; expected {identity.abbreviation!r}"
        )
    connection.execute(
        """
        INSERT INTO team_history (
            team_id,
            season,
            abbreviation,
            franchise_id,
            lookup_abbreviation
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(team_id, season) DO UPDATE SET
            abbreviation = excluded.abbreviation,
            franchise_id = excluded.franchise_id,
            lookup_abbreviation = excluded.lookup_abbreviation
        """,
        (
            identity.team_id,
            season,
            identity.abbreviation,
            identity.franchise_id or identity.abbreviation.lower(),
            canonical_team_lookup_abbreviation(identity.abbreviation),
        ),
    )


def _upsert_team_history_for_games(
    connection: sqlite3.Connection,
    games: list[NormalizedGameRecord],
) -> None:
    seen: set[tuple[int, str]] = set()
    for game in games:
        for team_id, team_code in (
            (game.team_id, game.team),
            (game.opponent_team_id, game.opponent),
        ):
            key = (team_id, game.season)
            if key in seen:
                continue
            _upsert_team_history_for_scope(
                connection,
                team_id=team_id,
                team=team_code,
                season=game.season,
            )
            seen.add(key)


__all__ = [
    "GAME_CACHE_BUILD_VERSION",
    "REGULAR_SEASON",
    "list_cache_load_rows",
    "list_cached_team_seasons_from_db",
    "load_cache_load_row",
    "load_normalized_game_players_from_db",
    "load_normalized_games_from_db",
    "replace_team_season_normalized_rows",
]
