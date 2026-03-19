from __future__ import annotations

import hashlib
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.team_seasons import TeamSeasonScope
from wowy.nba.validation import validate_normalized_cache_batch

GAME_CACHE_BUILD_VERSION = "normalized-cache-v1"
REGULAR_SEASON = "Regular Season"


@dataclass(frozen=True)
class NormalizedCacheLoadRow:
    team: str
    season: str
    season_type: str
    source_path: str
    source_snapshot: str
    source_kind: str
    build_version: str
    refreshed_at: str
    games_row_count: int
    game_players_row_count: int


def initialize_game_cache_db(db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH) -> None:
    with _connect(db_path) as connection:
        _migrate_cache_schema_if_needed(connection)
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS normalized_games (
                game_id TEXT NOT NULL,
                season TEXT NOT NULL,
                game_date TEXT NOT NULL,
                team TEXT NOT NULL,
                opponent TEXT NOT NULL,
                is_home INTEGER NOT NULL,
                margin REAL NOT NULL,
                season_type TEXT NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (game_id, team, season, season_type)
            );

            CREATE INDEX IF NOT EXISTS idx_normalized_games_game_id
            ON normalized_games (game_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_games_season_type_season_team
            ON normalized_games (season_type, season, team);

            CREATE INDEX IF NOT EXISTS idx_normalized_games_team_opponent_lookup
            ON normalized_games (season_type, season, team, opponent);

            CREATE INDEX IF NOT EXISTS idx_normalized_games_opponent_lookup
            ON normalized_games (season_type, season, opponent, team);

            CREATE TABLE IF NOT EXISTS normalized_game_players (
                game_id TEXT NOT NULL,
                season TEXT NOT NULL,
                season_type TEXT NOT NULL,
                team TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                appeared INTEGER NOT NULL,
                minutes REAL,
                PRIMARY KEY (game_id, team, player_id, season, season_type)
            );

            CREATE INDEX IF NOT EXISTS idx_normalized_game_players_game_id
            ON normalized_game_players (game_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_game_players_season_player
            ON normalized_game_players (season, player_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_game_players_season_type_season_team
            ON normalized_game_players (season_type, season, team);

            CREATE TABLE IF NOT EXISTS normalized_cache_loads (
                team TEXT NOT NULL,
                season TEXT NOT NULL,
                season_type TEXT NOT NULL,
                source_path TEXT NOT NULL,
                source_snapshot TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                build_version TEXT NOT NULL,
                refreshed_at TEXT NOT NULL,
                games_row_count INTEGER NOT NULL,
                game_players_row_count INTEGER NOT NULL,
                PRIMARY KEY (team, season, season_type)
            );

            CREATE INDEX IF NOT EXISTS idx_normalized_cache_loads_source_path
            ON normalized_cache_loads (source_path);
            """
        )


def replace_team_season_normalized_rows(
    db_path: Path,
    *,
    team: str,
    season: str,
    season_type: str,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    source_path: str,
    source_snapshot: str,
    source_kind: str,
    build_version: str = GAME_CACHE_BUILD_VERSION,
) -> None:
    initialize_game_cache_db(db_path)
    team = team.upper()
    season = canonicalize_season_string(season)
    season_type = canonicalize_season_type(season_type)
    validate_normalized_cache_batch(
        team=team,
        season=season,
        season_type=season_type,
        games=games,
        game_players=game_players,
    )
    refreshed_at = datetime.now(UTC).isoformat()

    with _connect(db_path) as connection:
        connection.execute("BEGIN")
        connection.execute(
            """
            DELETE FROM normalized_game_players
            WHERE team = ? AND season = ? AND season_type = ?
            """,
            (team, season, season_type),
        )
        connection.execute(
            """
            DELETE FROM normalized_games
            WHERE team = ? AND season = ? AND season_type = ?
            """,
            (team, season, season_type),
        )
        connection.executemany(
            """
            INSERT INTO normalized_games (
                game_id,
                season,
                game_date,
                team,
                opponent,
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
                    game.team,
                    game.opponent,
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
                team,
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
                    player.team,
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
                team,
                season,
                season_type,
                source_path,
                source_snapshot,
                source_kind,
                build_version,
                refreshed_at,
                games_row_count,
                game_players_row_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team, season, season_type) DO UPDATE SET
                source_path = excluded.source_path,
                source_snapshot = excluded.source_snapshot,
                source_kind = excluded.source_kind,
                build_version = excluded.build_version,
                refreshed_at = excluded.refreshed_at,
                games_row_count = excluded.games_row_count,
                game_players_row_count = excluded.game_players_row_count
            """,
            (
                team,
                season,
                season_type,
                source_path,
                source_snapshot,
                source_kind,
                build_version,
                refreshed_at,
                len(games),
                len(game_players),
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
    query = """
        SELECT
            game_id,
            season,
            game_date,
            team,
            opponent,
            is_home,
            margin,
            season_type,
            source
        FROM normalized_games
        WHERE season_type = ?
    """
    params: list[object] = [season_type]
    query, params = _append_in_filter(
        query,
        params,
        column="team",
        values=[team.upper() for team in teams or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="season",
        values=[canonicalize_season_string(season) for season in seasons or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="game_id",
        values=game_ids or [],
    )
    query += " ORDER BY season, game_date, team, game_id"

    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        NormalizedGameRecord(
            game_id=row["game_id"],
            season=row["season"],
            game_date=row["game_date"],
            team=row["team"],
            opponent=row["opponent"],
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
    query = """
        SELECT
            game_id,
            team,
            player_id,
            player_name,
            appeared,
            minutes
        FROM normalized_game_players
        WHERE season_type = ?
    """
    params: list[object] = [season_type]
    query, params = _append_in_filter(
        query,
        params,
        column="team",
        values=[team.upper() for team in teams or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="season",
        values=[canonicalize_season_string(season) for season in seasons or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="game_id",
        values=game_ids or [],
    )
    query += " ORDER BY season, team, game_id, player_id"

    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        NormalizedGamePlayerRecord(
            game_id=row["game_id"],
            team=row["team"],
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
    season_type = canonicalize_season_type(season_type)
    with _connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                team,
                season,
                season_type,
                source_path,
                source_snapshot,
                source_kind,
                build_version,
                refreshed_at,
                games_row_count,
                game_players_row_count
            FROM normalized_cache_loads
            WHERE team = ? AND season = ? AND season_type = ?
            """,
            (team.upper(), canonicalize_season_string(season), season_type),
        ).fetchone()
    if row is None:
        return None
    return NormalizedCacheLoadRow(
        team=row["team"],
        season=row["season"],
        season_type=row["season_type"],
        source_path=row["source_path"],
        source_snapshot=row["source_snapshot"],
        source_kind=row["source_kind"],
        build_version=row["build_version"],
        refreshed_at=row["refreshed_at"],
        games_row_count=row["games_row_count"],
        game_players_row_count=row["game_players_row_count"],
    )

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
        SELECT DISTINCT team, season
        FROM normalized_games
    """
    params: list[object] = []
    if season_type is not None:
        query += " WHERE season_type = ?"
        params.append(season_type)
    query += " ORDER BY season, team"
    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        TeamSeasonScope(team=row["team"], season=row["season"])
        for row in rows
    ]


def build_normalized_cache_fingerprint(
    db_path: Path,
    *,
    season_type: str | None = None,
) -> str:
    initialize_game_cache_db(db_path)
    if season_type is not None:
        season_type = canonicalize_season_type(season_type)
    query = """
        SELECT
            team,
            season,
            season_type,
            source_path,
            source_snapshot,
            source_kind,
            build_version,
            games_row_count,
            game_players_row_count
        FROM normalized_cache_loads
    """
    params: list[object] = []
    if season_type is not None:
        query += " WHERE season_type = ?"
        params.append(season_type)
    query += " ORDER BY season_type, season, team"

    digest = hashlib.sha256()
    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    for row in rows:
        digest.update(row["team"].encode("utf-8"))
        digest.update(row["season"].encode("utf-8"))
        digest.update(row["season_type"].encode("utf-8"))
        digest.update(row["source_path"].encode("utf-8"))
        digest.update(row["source_snapshot"].encode("utf-8"))
        digest.update(row["source_kind"].encode("utf-8"))
        digest.update(row["build_version"].encode("utf-8"))
        digest.update(str(row["games_row_count"]).encode("utf-8"))
        digest.update(str(row["game_players_row_count"]).encode("utf-8"))
    return digest.hexdigest()


def ensure_explicit_regular_season_copy(
    source_path: Path,
    target_path: Path,
) -> bool:
    if source_path == target_path or not source_path.exists():
        return False
    if target_path.exists():
        source_stat = source_path.stat()
        target_stat = target_path.stat()
        if (
            source_stat.st_size == target_stat.st_size
            and source_stat.st_mtime_ns == target_stat.st_mtime_ns
        ):
            return False
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)
    return True


def build_file_snapshot(*paths: Path) -> str:
    parts = []
    for path in paths:
        stat = path.stat()
        parts.append(f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}")
    return "|".join(parts)


def _append_in_filter(
    query: str,
    params: list[object],
    *,
    column: str,
    values: list[object],
) -> tuple[str, list[object]]:
    if not values:
        return query, params
    query += f" AND {column} IN ({','.join('?' for _ in values)})"
    params.extend(values)
    return query, params


def _migrate_cache_schema_if_needed(connection: sqlite3.Connection) -> None:
    normalized_games_pk = _primary_key_columns(connection, "normalized_games")
    normalized_players_pk = _primary_key_columns(connection, "normalized_game_players")
    if normalized_games_pk and normalized_games_pk != [
        "game_id",
        "team",
        "season",
        "season_type",
    ]:
        connection.executescript(
            """
            DROP TABLE IF EXISTS normalized_cache_loads;
            DROP TABLE IF EXISTS normalized_game_players;
            DROP TABLE IF EXISTS normalized_games;
            """
        )
        return
    if normalized_players_pk and normalized_players_pk != [
        "game_id",
        "team",
        "player_id",
        "season",
        "season_type",
    ]:
        connection.executescript(
            """
            DROP TABLE IF EXISTS normalized_cache_loads;
            DROP TABLE IF EXISTS normalized_game_players;
            DROP TABLE IF EXISTS normalized_games;
            """
        )


def _primary_key_columns(connection: sqlite3.Connection, table_name: str) -> list[str]:
    table_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    if table_exists is None:
        return []
    columns = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [
        column["name"]
        for column in sorted(columns, key=lambda item: item["pk"])
        if column["pk"] > 0
    ]


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection
