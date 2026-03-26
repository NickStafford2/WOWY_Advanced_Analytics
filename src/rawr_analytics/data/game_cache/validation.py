from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Callable, TypeVar

from rawr_analytics.data.game_cache.rows import NormalizedCacheLoadRow
from rawr_analytics.data.player_metrics_db.validation import (
    _validate_iso_datetime,
    _validate_optional_non_negative_int,
    _validate_required_text,
)
from rawr_analytics.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.nba.normalize.validation import (
    _canonical_team_abbreviation,
    _validate_canonical_game,
    _validate_canonical_game_player,
    validate_normalized_cache_batch,
)
from rawr_analytics.nba.season_types import canonicalize_season_type
from rawr_analytics.nba.seasons import canonicalize_season_string
from rawr_analytics.nba.team_identity import (
    canonical_team_lookup_abbreviation,
    resolve_team_history_entry,
    resolve_team_id,
)

IssueT = TypeVar("IssueT")
IssueFactory = Callable[[str, str, str], IssueT]


def _validate_normalized_games_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    rows = connection.execute(
        """
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
        ORDER BY game.season_type, game.season, team_history.abbreviation, game.game_id
        """
    ).fetchall()
    for row in rows:
        key = (
            f"game_id={row['game_id']!r},team={row['team']!r},season={row['season']!r},"
            f"season_type={row['season_type']!r}"
        )
        if row["is_home"] not in {0, 1}:
            issues.append(
                issue_factory(
                    "normalized_games",
                    key,
                    f"is_home must be 0 or 1, got {row['is_home']!r}",
                )
            )
            continue
        game = NormalizedGameRecord(
            game_id=row["game_id"],
            season=row["season"],
            game_date=row["game_date"],
            team=row["team"],
            opponent=row["opponent"],
            is_home=bool(row["is_home"]),
            margin=row["margin"],
            season_type=row["season_type"],
            source=row["source"],
            team_id=row["team_id"],
            opponent_team_id=row["opponent_team_id"],
        )
        try:
            _validate_canonical_game(
                game,
                expected_team=game.team,
                expected_team_id=game.team_id,
                expected_season=game.season,
                expected_season_type=game.season_type,
            )
        except ValueError as exc:
            issues.append(issue_factory("normalized_games", key, str(exc)))


def _validate_normalized_game_players_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    rows = connection.execute(
        """
        SELECT
            player.game_id,
            player.season,
            player.season_type,
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
        ORDER BY
            player.season_type,
            player.season,
            team_history.abbreviation,
            player.game_id,
            player.player_id
        """
    ).fetchall()
    for row in rows:
        key = (
            f"game_id={row['game_id']!r},team={row['team']!r},player_id={row['player_id']!r},"
            f"season={row['season']!r},season_type={row['season_type']!r}"
        )
        try:
            if canonicalize_season_string(row["season"]) != row["season"]:
                raise ValueError("season must use canonical season format")
            if canonicalize_season_type(row["season_type"]) != row["season_type"]:
                raise ValueError("season_type must use canonical season type")
        except ValueError as exc:
            issues.append(issue_factory("normalized_game_players", key, str(exc)))
            continue
        if row["appeared"] not in {0, 1}:
            issues.append(
                issue_factory(
                    "normalized_game_players",
                    key,
                    f"appeared must be 0 or 1, got {row['appeared']!r}",
                )
            )
            continue
        player = NormalizedGamePlayerRecord(
            game_id=row["game_id"],
            team=row["team"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            appeared=bool(row["appeared"]),
            minutes=row["minutes"],
            team_id=row["team_id"],
        )
        try:
            _validate_canonical_game_player(
                player,
                expected_team=row["team"],
                expected_team_id=row["team_id"],
            )
        except ValueError as exc:
            issues.append(issue_factory("normalized_game_players", key, str(exc)))


def _validate_normalized_cache_loads_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    rows = connection.execute(
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
        ORDER BY load.season_type, load.season, load.team_id
        """
    ).fetchall()
    for row in rows:
        key = f"team={row['team']!r},season={row['season']!r},season_type={row['season_type']!r}"
        load_row = NormalizedCacheLoadRow(
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
        )
        try:
            if canonicalize_season_string(load_row.season) != load_row.season:
                raise ValueError("season must use canonical season format")
            if canonicalize_season_type(load_row.season_type) != load_row.season_type:
                raise ValueError("season_type must use canonical season type")
            _validate_required_text(load_row.team, "team")
            _canonical_team_abbreviation(load_row.team)
            _validate_optional_non_negative_int(load_row.team_id, "team_id")
            if load_row.team_id is None or load_row.team_id <= 0:
                raise ValueError("team_id must be a positive integer")
            if resolve_team_id(load_row.team, season=load_row.season) != load_row.team_id:
                raise ValueError("team_id does not match team abbreviation identity")
            _validate_required_text(load_row.source_path, "source_path")
            _validate_required_text(load_row.source_snapshot, "source_snapshot")
            _validate_required_text(load_row.source_kind, "source_kind")
            _validate_required_text(load_row.build_version, "build_version")
            _validate_iso_datetime(load_row.refreshed_at, "refreshed_at")
            _validate_optional_non_negative_int(load_row.games_row_count, "games_row_count")
            _validate_optional_non_negative_int(
                load_row.game_players_row_count,
                "game_players_row_count",
            )
        except ValueError as exc:
            issues.append(issue_factory("normalized_cache_loads", key, str(exc)))


def _validate_normalized_cache_relations(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    game_rows = connection.execute(
        """
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
        ORDER BY game.season_type, game.season, team_history.abbreviation, game.game_id
        """
    ).fetchall()
    player_rows = connection.execute(
        """
        SELECT
            player.game_id,
            player.season,
            player.season_type,
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
        ORDER BY
            player.season_type,
            player.season,
            team_history.abbreviation,
            player.game_id,
            player.player_id
        """
    ).fetchall()
    load_rows = connection.execute(
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
            load.game_players_row_count
        FROM normalized_cache_loads AS load
        JOIN team_history
          ON team_history.team_id = load.team_id
         AND team_history.season = load.season
        """
    ).fetchall()

    games_by_scope: dict[tuple[int, str, str], list[NormalizedGameRecord]] = defaultdict(list)
    players_by_scope: dict[tuple[int, str, str], list[NormalizedGamePlayerRecord]] = defaultdict(
        list
    )
    game_key_scope_map: dict[tuple[str, int], tuple[int, str, str]] = {}

    for row in game_rows:
        game = NormalizedGameRecord(
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
        scope = (game.team_id, game.season, game.season_type)
        games_by_scope[scope].append(game)
        game_key_scope_map[(game.game_id, game.team_id)] = scope

    for row in player_rows:
        player = NormalizedGamePlayerRecord(
            game_id=row["game_id"],
            team=row["team"],
            team_id=row["team_id"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            appeared=bool(row["appeared"]),
            minutes=row["minutes"],
        )
        scope = (row["team_id"], row["season"], row["season_type"])
        players_by_scope[scope].append(player)
        game_scope = game_key_scope_map.get((player.game_id, player.team_id))
        if game_scope is None:
            issues.append(
                issue_factory(
                    "normalized_game_players",
                    (
                        f"game_id={player.game_id!r},team={player.team!r},"
                        f"player_id={player.player_id!r}"
                    ),
                    "player row has no matching normalized_games row",
                )
            )
        elif game_scope != scope:
            issues.append(
                issue_factory(
                    "normalized_game_players",
                    (
                        f"game_id={player.game_id!r},team={player.team!r},"
                        f"player_id={player.player_id!r}"
                    ),
                    "player row season or season_type does not match normalized_games row",
                )
            )

    _validate_reciprocal_game_margins(game_rows, issues, issue_factory=issue_factory)

    game_scopes = set(games_by_scope)
    player_scopes = set(players_by_scope)
    load_scopes = {(row["team_id"], row["season"], row["season_type"]) for row in load_rows}

    for scope in sorted(game_scopes | player_scopes):
        team_id, season, season_type = scope
        games_for_scope = games_by_scope.get(scope, [])
        players_for_scope = players_by_scope.get(scope, [])
        team = games_for_scope[0].team if games_for_scope else players_for_scope[0].team
        try:
            validate_normalized_cache_batch(
                team=team,
                team_id=team_id,
                season=season,
                season_type=season_type,
                games=games_for_scope,
                game_players=players_for_scope,
            )
        except ValueError as exc:
            issues.append(
                issue_factory(
                    "normalized_cache_relations",
                    f"team={team!r},season={season!r},season_type={season_type!r}",
                    str(exc),
                )
            )

    for row in load_rows:
        scope = (row["team_id"], row["season"], row["season_type"])
        game_count = len(games_by_scope.get(scope, []))
        player_count = len(players_by_scope.get(scope, []))
        key = f"team_id={scope[0]!r},season={scope[1]!r},season_type={scope[2]!r}"
        if row["games_row_count"] != game_count:
            issues.append(
                issue_factory(
                    "normalized_cache_loads",
                    key,
                    (
                        "games_row_count does not match normalized_games count: "
                        f"{row['games_row_count']} != {game_count}"
                    ),
                )
            )
        if row["game_players_row_count"] != player_count:
            issues.append(
                issue_factory(
                    "normalized_cache_loads",
                    key,
                    (
                        "game_players_row_count does not match normalized_game_players count: "
                        f"{row['game_players_row_count']} != {player_count}"
                    ),
                )
            )

    for scope in sorted((game_scopes | player_scopes) - load_scopes):
        issues.append(
            issue_factory(
                "normalized_cache_loads",
                f"team={scope[0]!r},season={scope[1]!r},season_type={scope[2]!r}",
                "missing normalized_cache_loads row for existing normalized cache scope",
            )
        )


def _validate_reciprocal_game_margins(
    game_rows: list[sqlite3.Row],
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    games_by_id: dict[tuple[str, str, str], list[NormalizedGameRecord]] = defaultdict(list)

    for row in game_rows:
        games_by_id[(row["season"], row["season_type"], row["game_id"])].append(
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
        )

    for _, games in games_by_id.items():
        if len(games) != 2:
            continue

        first_game, second_game = games
        if (
            first_game.identity_team != second_game.identity_opponent
            or second_game.identity_team != first_game.identity_opponent
        ):
            issues.append(
                issue_factory(
                    "normalized_games",
                    (
                        f"game_id={first_game.game_id!r},season={first_game.season!r},"
                        f"season_type={first_game.season_type!r}"
                    ),
                    "paired game rows must reference each other as opponents",
                )
            )
        if first_game.is_home == second_game.is_home:
            issues.append(
                issue_factory(
                    "normalized_games",
                    (
                        f"game_id={first_game.game_id!r},season={first_game.season!r},"
                        f"season_type={first_game.season_type!r}"
                    ),
                    "paired game rows must have opposite home/away flags",
                )
            )
        if first_game.game_date != second_game.game_date:
            issues.append(
                issue_factory(
                    "normalized_games",
                    (
                        f"game_id={first_game.game_id!r},season={first_game.season!r},"
                        f"season_type={first_game.season_type!r}"
                    ),
                    "paired game rows must have the same game date",
                )
            )
        if first_game.margin != -second_game.margin:
            issues.append(
                issue_factory(
                    "normalized_games",
                    (
                        f"game_id={first_game.game_id!r},season={first_game.season!r},"
                        f"season_type={first_game.season_type!r}"
                    ),
                    (
                        "paired game rows must have opposite margins: "
                        f"{first_game.team}={first_game.margin} "
                        f"{second_game.team}={second_game.margin}"
                    ),
                )
            )


def _validate_team_history_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    rows = connection.execute(
        """
        SELECT team_id, season, abbreviation, franchise_id, lookup_abbreviation
        FROM team_history
        ORDER BY season, team_id
        """
    ).fetchall()
    for row in rows:
        key = f"team_id={row['team_id']!r},season={row['season']!r}"
        try:
            if canonicalize_season_string(row["season"]) != row["season"]:
                raise ValueError("season must use canonical season format")
            _validate_optional_non_negative_int(row["team_id"], "team_id")
            if row["team_id"] is None or row["team_id"] <= 0:
                raise ValueError("team_id must be a positive integer")
            _validate_required_text(row["abbreviation"], "abbreviation")
            _canonical_team_abbreviation(row["abbreviation"])
            _validate_required_text(row["franchise_id"], "franchise_id")
            _validate_required_text(row["lookup_abbreviation"], "lookup_abbreviation")
            expected_history = resolve_team_history_entry(row["abbreviation"], season=row["season"])
            if expected_history.team_id != row["team_id"]:
                raise ValueError("team_id does not match abbreviation history")
            if row["abbreviation"] != expected_history.abbreviation:
                raise ValueError("abbreviation does not match official team history")
            expected_franchise_id = (
                expected_history.franchise_id or expected_history.abbreviation.lower()
            )
            if row["franchise_id"] != expected_franchise_id:
                raise ValueError("franchise_id does not match official team history")
            if row["lookup_abbreviation"] != canonical_team_lookup_abbreviation(
                row["abbreviation"]
            ):
                raise ValueError("lookup_abbreviation does not match canonical lookup abbreviation")
        except ValueError as exc:
            issues.append(issue_factory("team_history", key, str(exc)))


__all__ = [
    "_validate_normalized_cache_loads_table",
    "_validate_normalized_cache_relations",
    "_validate_normalized_game_players_table",
    "_validate_normalized_games_table",
    "_validate_reciprocal_game_margins",
    "_validate_team_history_table",
]
