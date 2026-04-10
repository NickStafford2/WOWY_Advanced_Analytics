from __future__ import annotations

from datetime import UTC, datetime

from rawr_analytics.data._paths import NORMALIZED_CACHE_DB_PATH
from rawr_analytics.data.game_cache._queries import (
    replace_team_season_cache_rows,
    select_cache_load_rows,
    select_normalized_game_player_rows,
    select_normalized_game_rows,
)
from rawr_analytics.data.game_cache._records import (
    GameCacheSnapshot,
    TeamSeasonCacheEntry,
    build_cache_fingerprint,
    build_normalized_game_player_record,
    build_normalized_game_record,
    build_team_season_cache_entry,
)
from rawr_analytics.data.game_cache._schema import connect, initialize_game_cache_db
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

_GAME_CACHE_BUILD_VERSION = "normalized-cache-v3"


def store_team_season_cache(
    *,
    scope: TeamSeasonScope,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    source_path: str,
    source_snapshot: str,
    source_kind: str,
    build_version: str = _GAME_CACHE_BUILD_VERSION,
    expected_games_count: int | None = None,
    skipped_games_count: int | None = None,
) -> None:
    _require_team_season_records(scope=scope, games=games, game_players=game_players)
    initialize_game_cache_db()
    with connect(NORMALIZED_CACHE_DB_PATH) as connection:
        connection.execute("BEGIN")
        replace_team_season_cache_rows(
            connection,
            scope=scope,
            games=games,
            game_players=game_players,
            source_path=source_path,
            source_snapshot=source_snapshot,
            source_kind=source_kind,
            build_version=build_version,
            refreshed_at=datetime.now(UTC).isoformat(),
            expected_games_count=expected_games_count,
            skipped_games_count=skipped_games_count,
        )
        connection.commit()


def load_team_season_cache(
    team_seasons: list[TeamSeasonScope],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    if not team_seasons:
        raise ValueError("No team-season scopes were requested")
    _require_cached_team_season_scopes(team_seasons)

    initialize_game_cache_db()
    with connect(NORMALIZED_CACHE_DB_PATH) as connection:
        games = [
            build_normalized_game_record(row)
            for row in select_normalized_game_rows(connection, team_seasons=team_seasons)
        ]
        game_players = [
            build_normalized_game_player_record(row)
            for row in select_normalized_game_player_rows(connection, team_seasons=team_seasons)
        ]
    if not games or not game_players:
        raise ValueError("No database cache matched the requested scope")
    return games, game_players


def list_cached_scopes(
    *,
    seasons: list[Season] | None = None,
    teams: list[Team] | None = None,
) -> list[TeamSeasonScope]:
    entries = _list_cache_entries(seasons=seasons, teams=teams)
    return [entry.scope for entry in entries if entry.is_available]


def load_cache_snapshot() -> GameCacheSnapshot:  # todo? probably pass info if this is ever used.
    entries = _list_cache_entries()  # todo? probably pass info if this is ever used.
    return GameCacheSnapshot(
        # season_type=season_type,
        fingerprint=build_cache_fingerprint(entries),
        entries=entries,
    )


def _list_cache_entries(
    *,
    seasons: list[Season] | None = None,
    teams: list[Team] | None = None,
) -> list[TeamSeasonCacheEntry]:
    if not NORMALIZED_CACHE_DB_PATH.exists():
        return []
    initialize_game_cache_db()
    with connect(NORMALIZED_CACHE_DB_PATH) as connection:
        rows = select_cache_load_rows(
            connection,
            teams=teams,
            seasons=seasons,
        )
    return [build_team_season_cache_entry(row) for row in rows]


def _require_team_season_records(
    *,
    scope: TeamSeasonScope,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
) -> None:
    game_ids = {game.game_id for game in games}
    for game in games:
        assert game.team == scope.team, "game cache writes must use a single team-season scope"
        assert game.season == scope.season, "game cache writes must use a single team-season scope"
    for game_player in game_players:
        assert game_player.team == scope.team, "player cache writes must use a single team scope"
        assert game_player.game_id in game_ids, "player cache writes must reference stored games"


def _require_cached_team_season_scopes(team_seasons: list[TeamSeasonScope]) -> None:
    cached_scope_keys = _team_season_keys(
        list_cached_scopes(
            teams=[scope.team for scope in team_seasons],
            seasons=[scope.season for scope in team_seasons],
        )
    )
    for scope in team_seasons:
        if _team_season_key(scope) in cached_scope_keys:
            continue
        raise ValueError(
            "Missing cached team-season scope for "
            f"{scope.team.abbreviation(season=scope.season)} {scope.season}"
        )


def _team_season_key(scope: TeamSeasonScope) -> tuple[int, str, str]:
    return (
        scope.team.team_id,
        scope.season.year_string_nba_api,
        scope.season.season_type.value,
    )


def _team_season_keys(team_seasons: list[TeamSeasonScope]) -> set[tuple[int, str, str]]:
    return {_team_season_key(scope) for scope in team_seasons}


__all__ = [
    "GameCacheSnapshot",
    "TeamSeasonCacheEntry",
    "list_cached_scopes",
    "load_cache_snapshot",
    "load_team_season_cache",
    "store_team_season_cache",
]
