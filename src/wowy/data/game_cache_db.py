from __future__ import annotations

import hashlib
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.ingest.validation import validate_normalized_cache_batch
from wowy.nba.models import CanonicalGamePlayerRecord, CanonicalGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.team_identity import (
    canonical_team_lookup_abbreviation,
    resolve_team_id,
    resolve_team_identity_from_id_and_season,
)
from wowy.nba.team_seasons import TeamSeasonScope

GAME_CACHE_BUILD_VERSION = "normalized-cache-v2"
REGULAR_SEASON = "Regular Season"


@dataclass(frozen=True)
class NormalizedCacheLoadRow:
    team: str
    team_id: int
    season: str
    season_type: str
    source_path: str
    source_snapshot: str
    source_kind: str
    build_version: str
    refreshed_at: str
    games_row_count: int
    game_players_row_count: int
    expected_games_row_count: int | None = None
    skipped_games_row_count: int | None = None


def initialize_game_cache_db(db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH) -> None:
    with _connect(db_path) as connection:
        _migrate_cache_schema_if_needed(connection)
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS team_history (
                team_id INTEGER NOT NULL,
                season TEXT NOT NULL,
                abbreviation TEXT NOT NULL,
                franchise_id TEXT NOT NULL,
                lookup_abbreviation TEXT NOT NULL,
                PRIMARY KEY (team_id, season),
                UNIQUE (season, abbreviation)
            );

            CREATE INDEX IF NOT EXISTS idx_team_history_season_lookup
            ON team_history (season, lookup_abbreviation);

            CREATE TABLE IF NOT EXISTS normalized_games (
                game_id TEXT NOT NULL,
                season TEXT NOT NULL,
                game_date TEXT NOT NULL,
                team_id INTEGER NOT NULL,
                opponent_team_id INTEGER NOT NULL,
                is_home INTEGER NOT NULL,
                margin REAL NOT NULL,
                season_type TEXT NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (game_id, team_id, season, season_type)
            );

            CREATE INDEX IF NOT EXISTS idx_normalized_games_game_id
            ON normalized_games (game_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_games_season_type_season_team
            ON normalized_games (season_type, season, team_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_games_team_opponent_lookup
            ON normalized_games (season_type, season, team_id, opponent_team_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_games_opponent_lookup
            ON normalized_games (season_type, season, opponent_team_id, team_id);

            CREATE TABLE IF NOT EXISTS normalized_game_players (
                game_id TEXT NOT NULL,
                season TEXT NOT NULL,
                season_type TEXT NOT NULL,
                team_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                appeared INTEGER NOT NULL,
                minutes REAL,
                PRIMARY KEY (game_id, team_id, player_id, season, season_type)
            );

            CREATE INDEX IF NOT EXISTS idx_normalized_game_players_game_id
            ON normalized_game_players (game_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_game_players_season_player
            ON normalized_game_players (season, player_id);

            CREATE INDEX IF NOT EXISTS idx_normalized_game_players_season_type_season_team
            ON normalized_game_players (season_type, season, team_id);

            CREATE TABLE IF NOT EXISTS normalized_cache_loads (
                team_id INTEGER NOT NULL,
                season TEXT NOT NULL,
                season_type TEXT NOT NULL,
                source_path TEXT NOT NULL,
                source_snapshot TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                build_version TEXT NOT NULL,
                refreshed_at TEXT NOT NULL,
                games_row_count INTEGER NOT NULL,
                game_players_row_count INTEGER NOT NULL,
                expected_games_row_count INTEGER,
                skipped_games_row_count INTEGER,
                PRIMARY KEY (team_id, season, season_type)
            );

            CREATE INDEX IF NOT EXISTS idx_normalized_cache_loads_source_path
            ON normalized_cache_loads (source_path);
            """
        )


def replace_team_season_normalized_rows(
    db_path: Path,
    *,
    team: str,
    team_id: int | None = None,
    season: str,
    season_type: str,
    games: list[CanonicalGameRecord],
    game_players: list[CanonicalGamePlayerRecord],
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
    team_id = team_id or resolve_team_id(team, season=season)
    canonical_team = resolve_team_identity_from_id_and_season(team_id, season).abbreviation
    season_type = canonicalize_season_type(season_type)
    games = [_with_resolved_game_identity(game) for game in games]
    game_players = [
        _with_resolved_player_identity(
            player,
            default_team=canonical_team,
            season=season,
        )
        for player in game_players
    ]
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
) -> list[CanonicalGameRecord]:
    initialize_game_cache_db(db_path)
    season_type = canonicalize_season_type(season_type)
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
        values=_resolve_team_ids(teams),
    )
    query, params = _append_in_filter(
        query,
        params,
        column="game.season",
        values=[canonicalize_season_string(season) for season in seasons or []],
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
        CanonicalGameRecord(
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
) -> list[CanonicalGamePlayerRecord]:
    initialize_game_cache_db(db_path)
    season_type = canonicalize_season_type(season_type)
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
        values=_resolve_team_ids(teams),
    )
    query, params = _append_in_filter(
        query,
        params,
        column="player.season",
        values=[canonicalize_season_string(season) for season in seasons or []],
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
        CanonicalGamePlayerRecord(
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
    team_id = resolve_team_id(team)
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
        values=[canonicalize_season_string(season) for season in seasons or []],
    )
    query, params = _append_in_filter(
        query,
        params,
        column="load.team_id",
        values=_resolve_team_ids(teams),
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
            team_history.abbreviation AS team,
            load.team_id,
            load.season,
            load.season_type,
            load.source_path,
            load.source_snapshot,
            load.source_kind,
            load.build_version,
            load.games_row_count,
            load.game_players_row_count,
            load.expected_games_row_count,
            load.skipped_games_row_count
        FROM normalized_cache_loads AS load
        JOIN team_history
          ON team_history.team_id = load.team_id
         AND team_history.season = load.season
    """
    params: list[object] = []
    if season_type is not None:
        query += " WHERE load.season_type = ?"
        params.append(season_type)
    query += " ORDER BY load.season_type, load.season, load.team_id"

    digest = hashlib.sha256()
    with _connect(db_path) as connection:
        rows = connection.execute(query, params).fetchall()
    for row in rows:
        digest.update(row["team"].encode("utf-8"))
        digest.update(str(row["team_id"]).encode("utf-8"))
        digest.update(row["season"].encode("utf-8"))
        digest.update(row["season_type"].encode("utf-8"))
        digest.update(row["source_path"].encode("utf-8"))
        digest.update(row["source_snapshot"].encode("utf-8"))
        digest.update(row["source_kind"].encode("utf-8"))
        digest.update(row["build_version"].encode("utf-8"))
        digest.update(str(row["games_row_count"]).encode("utf-8"))
        digest.update(str(row["game_players_row_count"]).encode("utf-8"))
        digest.update(str(row["expected_games_row_count"]).encode("utf-8"))
        digest.update(str(row["skipped_games_row_count"]).encode("utf-8"))
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


def _resolve_team_ids(teams: list[str] | None) -> list[int]:
    if not teams:
        return []
    return [resolve_team_id(team) for team in teams]


def _with_resolved_game_identity(game: CanonicalGameRecord) -> CanonicalGameRecord:
    team_id = game.team_id or resolve_team_id(game.team, game_date=game.game_date)
    opponent_team_id = (
        game.opponent_team_id or resolve_team_id(game.opponent, game_date=game.game_date)
    )
    return CanonicalGameRecord(
        game_id=game.game_id,
        season=game.season,
        game_date=game.game_date,
        team=game.team,
        opponent=game.opponent,
        is_home=game.is_home,
        margin=game.margin,
        season_type=game.season_type,
        source=game.source,
        team_id=team_id,
        opponent_team_id=opponent_team_id,
    )


def _with_resolved_player_identity(
    player: CanonicalGamePlayerRecord,
    *,
    default_team: str,
    season: str,
) -> CanonicalGamePlayerRecord:
    player_team = player.team or default_team
    return CanonicalGamePlayerRecord(
        game_id=player.game_id,
        team=player_team,
        player_id=player.player_id,
        player_name=player.player_name,
        appeared=player.appeared,
        minutes=player.minutes,
        team_id=player.team_id or resolve_team_id(player_team, season=season),
    )


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
    games: list[CanonicalGameRecord],
) -> None:
    seen: set[tuple[int, str]] = set()
    for game in games:
        if game.team_id is None or game.opponent_team_id is None:
            raise ValueError(f"Game {game.game_id!r} is missing resolved team ids")
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


def _migrate_cache_schema_if_needed(connection: sqlite3.Connection) -> None:
    normalized_games_pk = _primary_key_columns(connection, "normalized_games")
    normalized_players_pk = _primary_key_columns(connection, "normalized_game_players")
    normalized_cache_loads_pk = _primary_key_columns(connection, "normalized_cache_loads")
    team_history_pk = _primary_key_columns(connection, "team_history")
    expected_pks = (
        (
            normalized_games_pk,
            ["game_id", "team_id", "season", "season_type"],
        ),
        (
            normalized_players_pk,
            ["game_id", "team_id", "player_id", "season", "season_type"],
        ),
        (
            normalized_cache_loads_pk,
            ["team_id", "season", "season_type"],
        ),
        (
            team_history_pk,
            ["team_id", "season"],
        ),
    )
    if any(actual and actual != expected for actual, expected in expected_pks):
        _drop_cache_tables(connection)
        return

    required_columns = (
        ("team_history", "team_id"),
        ("team_history", "season"),
        ("team_history", "abbreviation"),
        ("team_history", "franchise_id"),
        ("team_history", "lookup_abbreviation"),
        ("normalized_games", "team_id"),
        ("normalized_games", "opponent_team_id"),
        ("normalized_game_players", "team_id"),
        ("normalized_cache_loads", "team_id"),
    )
    if any(
        _table_exists(connection, table_name)
        and not _table_has_column(connection, table_name, column_name)
        for table_name, column_name in required_columns
    ):
        _drop_cache_tables(connection)
        return

    deprecated_columns = (
        ("normalized_games", "team"),
        ("normalized_games", "opponent"),
        ("normalized_game_players", "team"),
        ("normalized_cache_loads", "team"),
    )
    if any(
        _table_exists(connection, table_name)
        and _table_has_column(connection, table_name, column_name)
        for table_name, column_name in deprecated_columns
    ):
        _drop_cache_tables(connection)
        return

    if _table_exists(connection, "normalized_cache_loads") and not _table_has_column(
        connection,
        "normalized_cache_loads",
        "expected_games_row_count",
    ):
        connection.execute(
            """
            ALTER TABLE normalized_cache_loads
            ADD COLUMN expected_games_row_count INTEGER
            """
        )
    if _table_exists(connection, "normalized_cache_loads") and not _table_has_column(
        connection,
        "normalized_cache_loads",
        "skipped_games_row_count",
    ):
        connection.execute(
            """
            ALTER TABLE normalized_cache_loads
            ADD COLUMN skipped_games_row_count INTEGER
            """
        )


def _drop_cache_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        DROP TABLE IF EXISTS normalized_cache_loads;
        DROP TABLE IF EXISTS normalized_game_players;
        DROP TABLE IF EXISTS normalized_games;
        DROP TABLE IF EXISTS team_history;
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


def _table_has_column(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
) -> bool:
    if not _table_exists(connection, table_name):
        return False
    columns = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(column["name"] == column_name for column in columns)


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    table_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return table_exists is not None


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection
