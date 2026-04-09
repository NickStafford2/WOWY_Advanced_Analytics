from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from typing import cast

from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.player import PlayerSummary
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


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


def build_team_season_cache_entry(row: sqlite3.Row) -> TeamSeasonCacheEntry:
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


def build_cache_fingerprint(entries: list[TeamSeasonCacheEntry]) -> str:
    digest = hashlib.sha256()
    for entry in entries:
        digest.update(str(entry.scope.team.team_id).encode("utf-8"))
        digest.update(entry.scope.season.year_string_nba_api.encode("utf-8"))
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


def build_normalized_game_record(row: sqlite3.Row) -> NormalizedGameRecord:
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


def build_normalized_game_player_record(
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
