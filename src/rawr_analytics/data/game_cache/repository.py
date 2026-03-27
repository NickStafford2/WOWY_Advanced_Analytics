from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime

from rawr_analytics.data.constants import DB_PATH
from rawr_analytics.data.game_cache.rows import NormalizedCacheLoadRow
from rawr_analytics.data.game_cache.schema import _connect, initialize_game_cache_db
from rawr_analytics.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.nba.normalize.validation import validate_normalized_cache_batch
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

_GAME_CACHE_BUILD_VERSION = "normalized-cache-v2"


def replace_team_season_normalized_rows(
    team: Team,
    season: Season,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    source_path: str,
    source_snapshot: str,
    source_kind: str,
    build_version: str = _GAME_CACHE_BUILD_VERSION,
    expected_games_row_count: int | None = None,
    skipped_games_row_count: int | None = None,
) -> None:
    initialize_game_cache_db()
    if team.team_id <= 0:
        raise ValueError(f"team_id must be positive for normalized cache writes: {team.team_id!r}")
    validate_normalized_cache_batch(
        team=team,
        season=season,
        games=games,
        game_players=game_players,
    )
    refreshed_at = datetime.now(UTC).isoformat()

    with _connect(DB_PATH) as connection:
        connection.execute("BEGIN")
        _upsert_team_history_for_scope(
            connection,
            team=team,
            season=season,
        )
        _upsert_team_history_for_games(connection, games)
        connection.execute(
            """
            DELETE FROM normalized_game_players
            WHERE team_id = ? AND season = ? AND season_type = ?
            """,
            (team.team_id, season.id, season.season_type.value),
        )
        connection.execute(
            """
            DELETE FROM normalized_games
            WHERE team_id = ? AND season = ? AND season_type = ?
            """,
            (team.team_id, season.id, season.season_type.value),
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
                    game.team.team_id,
                    game.opponent_team.team_id,
                    int(game.is_home),
                    game.margin,
                    game.season.season_type,
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
                    season.season_type.value,
                    player.team.team_id,
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
                team.team_id,
                season.id,
                season.season_type.value,
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
    *,
    teams: list[Team] = [],
    seasons: list[Season] = [],
    game_ids: list[str] = [],
) -> list[NormalizedGameRecord]:
    season_ids = [season.id for season in seasons or []]
    team_ids = [team.team_id for team in teams or []]
    initialize_game_cache_db()
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
    params: list[object] = [
        seasons[0].season_type.value
    ]  # todo: fix to get all season types in conjunction with seasons and teams.
    query, params = _append_in_filter(
        query,
        params,
        column="game.team_id",
        values=team_ids,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="game.season",
        values=season_ids,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="game.game_id",
        values=game_ids or [],
    )
    query += " ORDER BY game.season, game.game_date, team_history.abbreviation, game.game_id"

    with _connect(DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        NormalizedGameRecord(
            game_id=row["game_id"],
            season=Season(row["season"], row["season_type"]),
            game_date=row["game_date"],
            team=Team.from_id(row["team_id"]),
            opponent_team=Team.from_id(row["opponent_team_id"]),
            is_home=bool(row["is_home"]),
            margin=row["margin"],
            source=row["source"],
        )
        for row in rows
    ]


def load_normalized_game_players_from_db(
    *,
    season_type: str,
    teams: list[Team] = [],
    seasons: list[Season] = [],
    game_ids: list[str] = [],
) -> list[NormalizedGamePlayerRecord]:
    season_ids = [season.id for season in seasons or []]
    team_ids = [team.team_id for team in teams or []]
    initialize_game_cache_db()
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
        values=team_ids,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="player.season",
        values=season_ids,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="player.game_id",
        values=game_ids or [],
    )
    query += " ORDER BY player.season, team_history.abbreviation, player.game_id, player.player_id"

    with _connect(DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        NormalizedGamePlayerRecord(
            game_id=row["game_id"],
            team=Team.from_id(row["team_id"]),
            season=Season(row["season"], row["season_type"]),
            player_id=row["player_id"],
            player_name=row["player_name"],
            appeared=bool(row["appeared"]),
            minutes=row["minutes"],
        )
        for row in rows
    ]


def load_cache_load_row(
    team: Team,
    season: Season,
) -> NormalizedCacheLoadRow | None:
    initialize_game_cache_db()
    with _connect(DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT
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
            (team.team_id, season.id, season.season_type.value),
        ).fetchone()
    if row is None:
        return None
    return NormalizedCacheLoadRow(
        team=Team.from_id(row["team_id"]),
        season=Season(row["season"], row["season_type"]),
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


def load_normalized_scope_records_from_db(
    team_seasons: list[tuple[Team, Season]],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    for team_season in team_seasons:
        _require_cached_team_season_scope(
            team_season=team_season,
            season_type=season_type,
        )

    games = load_normalized_games_from_db(
        team_ids=[ts.team.team_id for ts in team_seasons],
        seasons=sorted({team_season.season for team_season in team_seasons}),
    )
    game_players = load_normalized_game_players_from_db(
        season_type=season_type,
        team_ids=[ts.team_id for ts in team_seasons],
        seasons=sorted({team_season.season for team_season in team_seasons}),
    )
    games, game_players = _filter_records_to_team_seasons(
        games,
        game_players,
        team_seasons,
    )
    if games and game_players:
        return games, game_players
    raise ValueError("No database cache matched the requested scope")


def has_cached_team_season_scope(
    team: Team,
    season: Season,
) -> bool:
    cache_load_row = load_cache_load_row(team, season)
    return (
        cache_load_row is not None
        and cache_load_row.games_row_count > 0
        and cache_load_row.game_players_row_count > 0
    )


def list_cache_load_rows(
    seasons: list[Season],
    teams: list[Team],
) -> list[NormalizedCacheLoadRow]:
    if not DB_PATH.exists():
        return []
    team_ids = [team.team_id for team in teams or []]
    season_ids = [season.id for season in seasons or []]
    season_types = [season.season_type.value for season in seasons or []]
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
    # if season_type is not None:
    #     query += " AND load.season_type = ?"
    #     params.append())
    query, params = _append_in_filter(
        query,
        params,
        column="load.season_type",
        values=season_types,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="load.season",
        values=season_ids,
    )
    query, params = _append_in_filter(
        query,
        params,
        column="load.team_id",
        values=team_ids,
    )
    query += " ORDER BY load.season, load.team_id"
    with _connect(DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        NormalizedCacheLoadRow(
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


def list_cached_team_seasons(season_type: str | None = None) -> list[TeamSeasonScope]:
    if not DB_PATH.exists():
        return []
    initialize_game_cache_db()
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
    with _connect(DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()
    return [tuple(Team.from_id(row["team_id"]), Season(season=row["season"], season_type=row["season_type"]) for row in rows]


def _filter_records_to_team_seasons(
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    team_seasons: list[tuple[Team, Season]],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    allowed_team_seasons = {
        (team_season.team_id, team_season.season) for team_season in team_seasons
    }
    filtered_games = [game for game in games if (game.team_id, game.season) in allowed_team_seasons]
    allowed_game_teams = {(game.game_id, game.team_id) for game in filtered_games}
    filtered_game_players = [
        player for player in game_players if (player.game_id, player.team_id) in allowed_game_teams
    ]
    return filtered_games, filtered_game_players


def _require_cached_team_season_scope(team=Team, season=Season) -> None:
    if has_cached_team_season_scope(team=team, season=season):
        return

    raise ValueError(f"Missing cached team-season scope for {team} {season}")


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


# I don't think I need this with the new Teams
# def _upsert_team_history_for_scope(
#     connection: sqlite3.Connection,
#     *,
#     team: Team,
#     season: Season,
# ) -> None:
#     connection.execute(
#         """
#         INSERT INTO team_history (
#             team_id,
#             season,
#             abbreviation,
#             franchise_id,
#             lookup_abbreviation
#         ) VALUES (?, ?, ?, ?, ?)
#         ON CONFLICT(team_id, season) DO UPDATE SET
#             abbreviation = excluded.abbreviation,
#             franchise_id = excluded.franchise_id,
#             lookup_abbreviation = excluded.lookup_abbreviation
#         """,
#         (
#             team.team_id,
#             season,
#             identity.abbreviation,
#             identity.franchise_id or identity.abbreviation.lower(),
#             canonical_team_lookup_abbreviation(identity.abbreviation),
#         ),
#     )


# what does this do?
def _upsert_team_history_for_games(
    connection: sqlite3.Connection,
    games: list[NormalizedGameRecord],
) -> None:
    seen: set[tuple[int, str]] = set()
    for game in games:
        for team_id, _ in (
            (game.team.team_id, game.team.team_id),
            (game.opponent_team.team_id, game.opponent_team.team_id),
        ):
            key = (team_id, game.season)
            if key in seen:
                continue
            _upsert_team_history_for_scope(
                connection,
                team=team,
                season=season,
            )
            seen.add(key)


__all__ = [
    "has_cached_team_season_scope",
    "list_cache_load_rows",
    "list_cached_team_seasons",
    "load_cache_load_row",
    "load_normalized_game_players_from_db",
    "load_normalized_games_from_db",
    "load_normalized_scope_records_from_db",
    "replace_team_season_normalized_rows",
]
