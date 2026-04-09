from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from rawr_analytics.data._paths import NORMALIZED_CACHE_DB_PATH
from rawr_analytics.data.game_cache._queries import (
    replace_team_season_cache_rows,
    select_cache_load_rows,
    select_normalized_game_player_rows,
    select_normalized_game_rows,
)
from rawr_analytics.data.game_cache._schema import connect, initialize_game_cache_db
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.player import PlayerSummary
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team

_GAME_CACHE_BUILD_VERSION = "normalized-cache-v3"


@dataclass(frozen=True)
class TeamSeasonCacheEntry:
    scope: TeamSeasonScope
    source_path: str
    source_snapshot: str
    source_kind: str
    build_version: str
    refreshed_at: str
    games_count: int
    game_players_count: int
    expected_games_count: int | None = None
    skipped_games_count: int | None = None

    @property
    def is_available(self) -> bool:
        return self.games_count > 0 and self.game_players_count > 0


@dataclass(frozen=True)
class GameCacheSnapshot:
    season_type: SeasonType
    fingerprint: str
    entries: list[TeamSeasonCacheEntry]

    @property
    def scopes(self) -> list[TeamSeasonScope]:
        return [entry.scope for entry in self.entries if entry.is_available]


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

    teams_by_id: dict[int, Team] = {}
    seasons_by_key: dict[tuple[str, str], Season] = {}
    for scope in team_seasons:
        teams_by_id[scope.team.team_id] = scope.team
        season_key = (scope.season.id, scope.season.season_type.value)
        seasons_by_key[season_key] = scope.season

    teams = [teams_by_id[team_id] for team_id in sorted(teams_by_id)]
    seasons = [
        seasons_by_key[key]
        for key in sorted(seasons_by_key, key=lambda item: (item[0], item[1]))
    ]
    initialize_game_cache_db()
    with connect(NORMALIZED_CACHE_DB_PATH) as connection:
        games = [
            _build_normalized_game_record(row)
            for row in select_normalized_game_rows(connection, teams=teams, seasons=seasons)
        ]
        game_players = [
            _build_normalized_game_player_record(row)
            for row in select_normalized_game_player_rows(connection, teams=teams, seasons=seasons)
        ]
    filtered_games, filtered_game_players = _filter_records_to_team_seasons(
        games=games,
        game_players=game_players,
        team_seasons=team_seasons,
    )
    if not filtered_games or not filtered_game_players:
        raise ValueError("No database cache matched the requested scope")
    return filtered_games, filtered_game_players


def list_cached_scopes(
    *,
    seasons: list[Season] | None = None,
    teams: list[Team] | None = None,
) -> list[TeamSeasonScope]:
    entries = _list_cache_entries(seasons=seasons, teams=teams)
    return [entry.scope for entry in entries if entry.is_available]


def load_cache_snapshot(season_type: SeasonType) -> GameCacheSnapshot:
    entries = _list_cache_entries()
    filtered_entries = [
        entry for entry in entries if entry.scope.season.season_type == season_type
    ]
    return GameCacheSnapshot(
        season_type=season_type,
        fingerprint=_build_cache_fingerprint(filtered_entries),
        entries=filtered_entries,
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
        rows = select_cache_load_rows(connection, teams=teams, seasons=seasons)
    return [_build_cache_entry(row) for row in rows]


def _build_cache_entry(row: sqlite3.Row) -> TeamSeasonCacheEntry:
    season_id = cast(str, row["season"])
    season_type = cast(str, row["season_type"])
    team_id = cast(int, row["team_id"])
    source_path = cast(str, row["source_path"])
    source_snapshot = cast(str, row["source_snapshot"])
    source_kind = cast(str, row["source_kind"])
    build_version = cast(str, row["build_version"])
    refreshed_at = cast(str, row["refreshed_at"])
    games_count = cast(int, row["games_row_count"])
    game_players_count = cast(int, row["game_players_row_count"])
    expected_games_count = cast(int | None, row["expected_games_row_count"])
    skipped_games_count = cast(int | None, row["skipped_games_row_count"])

    season = Season.parse(season_id, season_type)
    return TeamSeasonCacheEntry(
        scope=TeamSeasonScope(
            team=Team.from_id(team_id),
            season=season,
        ),
        source_path=source_path,
        source_snapshot=source_snapshot,
        source_kind=source_kind,
        build_version=build_version,
        refreshed_at=refreshed_at,
        games_count=games_count,
        game_players_count=game_players_count,
        expected_games_count=expected_games_count,
        skipped_games_count=skipped_games_count,
    )


def _build_cache_fingerprint(entries: list[TeamSeasonCacheEntry]) -> str:
    digest = hashlib.sha256()
    for entry in entries:
        digest.update(str(entry.scope.team.team_id).encode("utf-8"))
        digest.update(entry.scope.season.id.encode("utf-8"))
        digest.update(entry.scope.season.season_type.to_nba_format().encode("utf-8"))
        digest.update(entry.source_path.encode("utf-8"))
        digest.update(entry.source_snapshot.encode("utf-8"))
        digest.update(entry.source_kind.encode("utf-8"))
        digest.update(entry.build_version.encode("utf-8"))
        digest.update(str(entry.games_count).encode("utf-8"))
        digest.update(str(entry.game_players_count).encode("utf-8"))
        digest.update(str(entry.expected_games_count).encode("utf-8"))
        digest.update(str(entry.skipped_games_count).encode("utf-8"))
    return digest.hexdigest()


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
    cached_scope_keys = _team_season_keys(list_cached_scopes())
    for scope in team_seasons:
        if _team_season_key(scope) in cached_scope_keys:
            continue
        raise ValueError(
            "Missing cached team-season scope for "
            f"{scope.team.abbreviation(season=scope.season)} {scope.season}"
        )


def _build_normalized_game_record(row: sqlite3.Row) -> NormalizedGameRecord:
    game_id = cast(str, row["game_id"])
    game_date = cast(str, row["game_date"])
    season_id = cast(str, row["season"])
    season_type = cast(str, row["season_type"])
    team_id = cast(int, row["team_id"])
    opponent_team_id = cast(int, row["opponent_team_id"])
    is_home = cast(int, row["is_home"])
    margin = cast(int | float, row["margin"])
    source = cast(str, row["source"])

    return NormalizedGameRecord(
        game_id=game_id,
        game_date=game_date,
        season=Season.parse(season_id, season_type),
        team=Team.from_id(team_id),
        opponent_team=Team.from_id(opponent_team_id),
        is_home=bool(is_home),
        margin=float(margin),
        source=source,
    )


def _build_normalized_game_player_record(
    row: sqlite3.Row,
) -> NormalizedGamePlayerRecord:
    game_id = cast(str, row["game_id"])
    player_id = cast(int, row["player_id"])
    player_name = cast(str, row["player_name"])
    appeared = cast(int, row["appeared"])
    minutes = cast(int | float | None, row["minutes"])
    team_id = cast(int, row["team_id"])

    return NormalizedGamePlayerRecord(
        game_id=game_id,
        player=PlayerSummary(
            player_id=player_id,
            player_name=player_name,
        ),
        appeared=bool(appeared),
        minutes=None if minutes is None else float(minutes),
        team=Team.from_id(team_id),
    )


def _filter_records_to_team_seasons(
    *,
    games: list[NormalizedGameRecord],
    game_players: list[NormalizedGamePlayerRecord],
    team_seasons: list[TeamSeasonScope],
) -> tuple[list[NormalizedGameRecord], list[NormalizedGamePlayerRecord]]:
    allowed = _team_season_keys(team_seasons)
    filtered_games = [game for game in games if _game_team_season_key(game) in allowed]
    allowed_game_keys = {(game.game_id, game.team.team_id) for game in filtered_games}
    filtered_players = [
        player
        for player in game_players
        if (player.game_id, player.team.team_id) in allowed_game_keys
    ]
    return filtered_games, filtered_players


def _game_team_season_key(game: NormalizedGameRecord) -> tuple[int, str, str]:
    return (
        game.team.team_id,
        game.season.id,
        game.season.season_type.value,
    )


def _team_season_key(scope: TeamSeasonScope) -> tuple[int, str, str]:
    return (
        scope.team.team_id,
        scope.season.id,
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
