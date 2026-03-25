from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

from rawr_analytics.data.game_cache.schema import _connect, initialize_game_cache_db
from rawr_analytics.nba.season_types import canonicalize_season_type


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


def _ensure_explicit_regular_season_copy(
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


def _build_file_snapshot(*paths: Path) -> str:
    parts = []
    for path in paths:
        stat = path.stat()
        parts.append(f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}")
    return "|".join(parts)


__all__ = ["build_normalized_cache_fingerprint"]
