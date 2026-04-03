from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from rawr_analytics.data._paths import NORMALIZED_CACHE_DB_PATH
from rawr_analytics.data.game_cache.rows import (
    NormalizedCacheLoadRow,
    NormalizedGamePlayerRow,
    NormalizedGameRow,
)
from rawr_analytics.data.game_cache.schema import connect, initialize_game_cache_db
from rawr_analytics.shared.player import PlayerSummary
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

_GAME_CACHE_BUILD_VERSION = "normalized-cache-v3"


def replace_team_season_normalized_rows(
    team: Team,
    season: Season,
    games: list[NormalizedGameRow],
    game_players: list[NormalizedGamePlayerRow],
    source_path: str,
    source_snapshot: str,
    source_kind: str,
    build_version: str = _GAME_CACHE_BUILD_VERSION,
    expected_games_row_count: int | None = None,
    skipped_games_row_count: int | None = None,
) -> None:
    initialize_game_cache_db()
    refreshed_at = datetime.now(UTC).isoformat()
    season_type = _season_type_value(season)

    with connect(NORMALIZED_CACHE_DB_PATH) as connection:
        connection.execute("BEGIN")
        connection.execute(
            """
            DELETE FROM normalized_game_players
            WHERE team_id = ? AND season = ? AND season_type = ?
            """,
            (team.team_id, season.id, season_type),
        )
        connection.execute(
            """
            DELETE FROM normalized_games
            WHERE team_id = ? AND season = ? AND season_type = ?
            """,
            (team.team_id, season.id, season_type),
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
                    game.season.id,
                    game.game_date,
                    game.team.team_id,
                    game.opponent_team.team_id,
                    int(game.is_home),
                    game.margin,
                    _season_type_value(game.season),
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
                    season.id,
                    season_type,
                    player.team.team_id,
                    player.player.player_id,
                    player.player.player_name,
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
    *,
    teams: list[Team] | None = None,
    seasons: list[Season] | None = None,
    game_ids: list[str] | None = None,
) -> list[NormalizedGameRow]:
    initialize_game_cache_db()
    team_ids = [team.team_id for team in teams or []]
    season_ids = [season.id for season in seasons or []]
    season_types = sorted({_season_type_value(season) for season in seasons or []})

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
    query, params = _append_in_filter(query, params, column="team_id", values=team_ids)
    query, params = _append_in_filter(query, params, column="season", values=season_ids)
    query, params = _append_in_filter(query, params, column="season_type", values=season_types)
    query, params = _append_in_filter(query, params, column="game_id", values=game_ids or [])
    query += " ORDER BY season, game_date, team_id, game_id"

    with connect(NORMALIZED_CACHE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()

    games = [
        NormalizedGameRow(
            game_id=row["game_id"],
            game_date=row["game_date"],
            season=Season(row["season"], row["season_type"]),
            team=Team.from_id(row["team_id"]),
            opponent_team=Team.from_id(row["opponent_team_id"]),
            is_home=bool(row["is_home"]),
            margin=row["margin"],
            source=row["source"],
        )
        for row in rows
    ]
    if teams and seasons:
        allowed = {(team.team_id, season.id) for team in teams for season in seasons}
        games = [game for game in games if (game.team.team_id, game.season.id) in allowed]
    return games


def _load_normalized_game_players_from_db(
    *,
    teams: list[Team] | None = None,
    seasons: list[Season] | None = None,
    game_ids: list[str] | None = None,
) -> list[NormalizedGamePlayerRow]:
    initialize_game_cache_db()
    team_ids = [team.team_id for team in teams or []]
    season_ids = [season.id for season in seasons or []]
    season_types = sorted({_season_type_value(season) for season in seasons or []})

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
    query, params = _append_in_filter(query, params, column="team_id", values=team_ids)
    query, params = _append_in_filter(query, params, column="season", values=season_ids)
    query, params = _append_in_filter(query, params, column="season_type", values=season_types)
    query, params = _append_in_filter(query, params, column="game_id", values=game_ids or [])
    query += " ORDER BY season, team_id, game_id, player_id"

    with connect(NORMALIZED_CACHE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()

    players = [
        NormalizedGamePlayerRow(
            game_id=row["game_id"],
            player=PlayerSummary(
                player_id=row["player_id"],
                player_name=row["player_name"],
            ),
            appeared=bool(row["appeared"]),
            minutes=row["minutes"],
            team=Team.from_id(row["team_id"]),
        )
        for row in rows
    ]
    if teams and seasons:
        allowed_team_seasons = {(team.team_id, season.id) for team in teams for season in seasons}
        allowed_game_keys = {
            (row["game_id"], row["team_id"])
            for row in rows
            if (row["team_id"], row["season"]) in allowed_team_seasons
        }
        players = [
            player
            for player in players
            if (player.game_id, player.team.team_id) in allowed_game_keys
        ]
    return players


def _load_cache_load_row(
    team: Team,
    season: Season,
) -> NormalizedCacheLoadRow | None:
    initialize_game_cache_db()
    with connect(NORMALIZED_CACHE_DB_PATH) as connection:
        row = connection.execute(
            """
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
            WHERE team_id = ? AND season = ? AND season_type = ?
            """,
            (team.team_id, season.id, _season_type_value(season)),
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
    team_seasons: list[TeamSeasonScope],
) -> tuple[list[NormalizedGameRow], list[NormalizedGamePlayerRow]]:
    for scope in team_seasons:
        _require_cached_team_season_scope(scope=scope)

    teams_by_id: dict[int, Team] = {}
    seasons_by_key: dict[tuple[str, str], Season] = {}
    for scope in team_seasons:
        teams_by_id[scope.team.team_id] = scope.team
        seasons_by_key[(scope.season.id, _season_type_value(scope.season))] = scope.season

    teams = [teams_by_id[team_id] for team_id in sorted(teams_by_id)]
    seasons = [
        seasons_by_key[key] for key in sorted(seasons_by_key, key=lambda item: (item[0], item[1]))
    ]
    games = load_normalized_games_from_db(teams=teams, seasons=seasons)
    game_players = _load_normalized_game_players_from_db(teams=teams, seasons=seasons)
    filtered_games, filtered_players = _filter_records_to_team_seasons(
        games=games,
        game_players=game_players,
        team_seasons=team_seasons,
    )
    if not filtered_games or not filtered_players:
        raise ValueError("No database cache matched the requested scope")
    return filtered_games, filtered_players


def _has_cached_team_season_scope(
    team: Team,
    season: Season,
) -> bool:
    row = _load_cache_load_row(team, season)
    return row is not None and row.games_row_count > 0 and row.game_players_row_count > 0


def list_cache_load_rows(
    *,
    seasons: list[Season] | None = None,
    teams: list[Team] | None = None,
) -> list[NormalizedCacheLoadRow]:
    if not NORMALIZED_CACHE_DB_PATH.exists():
        return []
    initialize_game_cache_db()
    team_ids = [team.team_id for team in teams or []]
    season_ids = [season.id for season in seasons or []]
    season_types = sorted({_season_type_value(season) for season in seasons or []})

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
    query, params = _append_in_filter(query, params, column="team_id", values=team_ids)
    query, params = _append_in_filter(query, params, column="season", values=season_ids)
    query, params = _append_in_filter(query, params, column="season_type", values=season_types)
    query += " ORDER BY season, team_id"

    with connect(NORMALIZED_CACHE_DB_PATH) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        NormalizedCacheLoadRow(
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
        for row in rows
    ]


def list_cached_team_seasons(
    *,
    seasons: list[Season] | None = None,
    teams: list[Team] | None = None,
) -> list[TeamSeasonScope]:
    rows = list_cache_load_rows(seasons=seasons, teams=teams)
    unique: dict[tuple[int, str, str], TeamSeasonScope] = {}
    for row in rows:
        key = (row.team.team_id, row.season.id, _season_type_value(row.season))
        unique[key] = TeamSeasonScope(team=row.team, season=row.season)
    return [unique[key] for key in sorted(unique, key=lambda item: (item[1], item[2], item[0]))]


def _filter_records_to_team_seasons(
    *,
    games: list[NormalizedGameRow],
    game_players: list[NormalizedGamePlayerRow],
    team_seasons: list[TeamSeasonScope],
) -> tuple[list[NormalizedGameRow], list[NormalizedGamePlayerRow]]:
    allowed = _team_season_keys(team_seasons)
    filtered_games = [game for game in games if (game.team.team_id, game.season.id) in allowed]
    allowed_game_keys = {(game.game_id, game.team.team_id) for game in filtered_games}
    filtered_players = [
        player
        for player in game_players
        if (player.game_id, player.team.team_id) in allowed_game_keys
    ]
    return filtered_games, filtered_players


def _require_cached_team_season_scope(
    *,
    scope: TeamSeasonScope,
) -> None:
    if _has_cached_team_season_scope(scope.team, scope.season):
        return
    raise ValueError(
        "Missing cached team-season scope for "
        f"{scope.team.abbreviation(season=scope.season)} {scope.season}"
    )


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


def _season_type_value(season: Season) -> str:
    return season.season_type.to_nba_format()


def _team_season_keys(team_seasons: Sequence[TeamSeasonScope]) -> set[tuple[int, str]]:
    return {(scope.team.team_id, scope.season.id) for scope in team_seasons}
