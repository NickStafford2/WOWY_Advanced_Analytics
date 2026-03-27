from __future__ import annotations

import hashlib

from rawr_analytics.data.constants import DB_PATH
from rawr_analytics.data.game_cache.schema import _connect, initialize_game_cache_db
from rawr_analytics.shared.season import Season


def build_normalized_cache_fingerprint(
    *,
    season: Season,
) -> str:
    initialize_game_cache_db()
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
    query += " WHERE load.season_type = ?"
    params.append(season.season_type.value)
    query += " ORDER BY load.season_type, load.season, load.team_id"

    digest = hashlib.sha256()
    with _connect(DB_PATH) as connection:
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


__all__ = ["build_normalized_cache_fingerprint"]
