from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from datetime import date

from rawr_analytics.data._validation import (
    _validate_iso_datetime,
    _validate_optional_non_negative_int,
    _validate_required_text,
)
from rawr_analytics.data._validation_issue import ValidationIssue
from rawr_analytics.data.game_cache.rows import (
    NormalizedGamePlayerRow,
    NormalizedGameRow,
)
from rawr_analytics.shared.player import PlayerSummary
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def validate_normalized_games_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    rows = connection.execute(
        """
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
        ORDER BY season_type, season, team_id, game_id
        """
    ).fetchall()
    for row in rows:
        key = (
            f"game_id={row['game_id']!r},team_id={row['team_id']!r},season={row['season']!r},"
            f"season_type={row['season_type']!r}"
        )
        try:
            if row["is_home"] not in {0, 1}:
                raise ValueError(f"is_home must be 0 or 1, got {row['is_home']!r}")
            game = _build_normalized_game_record(row)
            _validate_normalized_game_row(
                game,
                expected_team=game.team,
                expected_season=game.season,
            )
        except (AssertionError, ValueError) as exc:
            issues.append(ValidationIssue("normalized_games", key, str(exc)))


def validate_normalized_game_players_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    rows = connection.execute(
        """
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
        ORDER BY season_type, season, team_id, game_id, player_id
        """
    ).fetchall()
    for row in rows:
        key = (
            f"game_id={row['game_id']!r},team_id={row['team_id']!r},player_id={row['player_id']!r},"
            f"season={row['season']!r},season_type={row['season_type']!r}"
        )
        try:
            if row["appeared"] not in {0, 1}:
                raise ValueError(f"appeared must be 0 or 1, got {row['appeared']!r}")
            player, season = _build_normalized_game_player_record(row)
            _validate_normalized_game_player_row(player, expected_team=player.team)
            player.team.for_season(season)
        except (AssertionError, ValueError) as exc:
            issues.append(ValidationIssue("normalized_game_players", key, str(exc)))


def validate_normalized_cache_loads_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    rows = connection.execute(
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
        ORDER BY season_type, season, team_id
        """
    ).fetchall()
    for row in rows:
        key = (
            f"team_id={row['team_id']!r},season={row['season']!r},"
            f"season_type={row['season_type']!r}"
        )
        try:
            season = Season.parse(row["season"], row["season_type"])
            team = Team.from_id(row["team_id"])
            team.for_season(season)
            _validate_required_text(row["source_path"], "source_path")
            _validate_required_text(row["source_snapshot"], "source_snapshot")
            _validate_required_text(row["source_kind"], "source_kind")
            _validate_required_text(row["build_version"], "build_version")
            _validate_iso_datetime(row["refreshed_at"], "refreshed_at")
            _validate_optional_non_negative_int(row["games_row_count"], "games_row_count")
            _validate_optional_non_negative_int(
                row["game_players_row_count"],
                "game_players_row_count",
            )
            _validate_optional_non_negative_int(
                row["expected_games_row_count"],
                "expected_games_row_count",
            )
            _validate_optional_non_negative_int(
                row["skipped_games_row_count"],
                "skipped_games_row_count",
            )
        except (AssertionError, ValueError) as exc:
            issues.append(ValidationIssue("normalized_cache_loads", key, str(exc)))


def validate_normalized_cache_relations(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    game_rows = connection.execute(
        """
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
        ORDER BY season_type, season, team_id, game_id
        """
    ).fetchall()
    player_rows = connection.execute(
        """
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
        ORDER BY season_type, season, team_id, game_id, player_id
        """
    ).fetchall()
    load_rows = connection.execute(
        """
        SELECT
            team_id,
            season,
            season_type,
            games_row_count,
            game_players_row_count
        FROM normalized_cache_loads
        """
    ).fetchall()

    games_by_scope: dict[tuple[int, str, str], list[NormalizedGameRow]] = defaultdict(list)
    players_by_scope: dict[tuple[int, str, str], list[NormalizedGamePlayerRow]] = defaultdict(list)
    game_key_scope_map: dict[tuple[str, int], tuple[int, str, str]] = {}

    for row in game_rows:
        try:
            game = _build_normalized_game_record(row)
        except (AssertionError, ValueError):
            continue
        scope = (game.team.team_id, game.season.id, game.season.season_type.to_nba_format())
        games_by_scope[scope].append(game)
        game_key_scope_map[(game.game_id, game.team.team_id)] = scope

    for row in player_rows:
        try:
            player, season = _build_normalized_game_player_record(row)
        except (AssertionError, ValueError):
            continue
        scope = (player.team.team_id, season.id, season.season_type.to_nba_format())
        players_by_scope[scope].append(player)
        game_scope = game_key_scope_map.get((player.game_id, player.team.team_id))
        if game_scope is None:
            issues.append(
                ValidationIssue(
                    "normalized_game_players",
                    (
                        f"game_id={player.game_id!r},team_id={player.team.team_id!r},"
                        f"player_id={player.player.player_id!r}"
                    ),
                    "player row has no matching normalized_games row",
                )
            )
        elif game_scope != scope:
            issues.append(
                ValidationIssue(
                    "normalized_game_players",
                    (
                        f"game_id={player.game_id!r},team_id={player.team.team_id!r},"
                        f"player_id={player.player.player_id!r}"
                    ),
                    "player row season or season_type does not match normalized_games row",
                )
            )

    _validate_reciprocal_game_margins(game_rows, issues)

    game_scopes = set(games_by_scope)
    player_scopes = set(players_by_scope)
    load_scopes = {(row["team_id"], row["season"], row["season_type"]) for row in load_rows}

    for scope in sorted(game_scopes | player_scopes):
        team_id, season_id, season_type = scope
        try:
            _validate_normalized_scope_batch(
                team=Team.from_id(team_id),
                season=Season.parse(season_id, season_type),
                games=games_by_scope.get(scope, []),
                game_players=players_by_scope.get(scope, []),
            )
        except (AssertionError, ValueError) as exc:
            issues.append(
                ValidationIssue(
                    "normalized_cache_relations",
                    f"team_id={team_id!r},season={season_id!r},season_type={season_type!r}",
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
                ValidationIssue(
                    "normalized_cache_loads",
                    key,
                    "games_row_count does not match normalized_games count: "
                    f"{row['games_row_count']} != {game_count}",
                )
            )
        if row["game_players_row_count"] != player_count:
            issues.append(
                ValidationIssue(
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
            ValidationIssue(
                "normalized_cache_loads",
                f"team_id={scope[0]!r},season={scope[1]!r},season_type={scope[2]!r}",
                "missing normalized_cache_loads row for existing normalized cache scope",
            )
        )


def _validate_reciprocal_game_margins(
    game_rows: list[sqlite3.Row],
    issues: list[ValidationIssue],
) -> None:
    games_by_id: dict[tuple[str, str, str], list[NormalizedGameRow]] = defaultdict(list)

    for row in game_rows:
        try:
            game = _build_normalized_game_record(row)
        except (AssertionError, ValueError):
            continue
        games_by_id[(game.season.id, game.season.season_type.value, game.game_id)].append(game)

    for games in games_by_id.values():
        if len(games) != 2:
            continue

        first_game, second_game = games
        if first_game.team.team_id != second_game.opponent_team.team_id:
            issues.append(
                ValidationIssue(
                    "normalized_games",
                    (
                        f"game_id={first_game.game_id!r},season={first_game.season.id!r},"
                        f"season_type={first_game.season.season_type.value!r}"
                    ),
                    "paired game rows must reference each other as opponents",
                )
            )
        if second_game.team.team_id != first_game.opponent_team.team_id:
            issues.append(
                ValidationIssue(
                    "normalized_games",
                    (
                        f"game_id={first_game.game_id!r},season={first_game.season.id!r},"
                        f"season_type={first_game.season.season_type.value!r}"
                    ),
                    "paired game rows must reference each other as opponents",
                )
            )
        if first_game.is_home == second_game.is_home:
            issues.append(
                ValidationIssue(
                    "normalized_games",
                    (
                        f"game_id={first_game.game_id!r},season={first_game.season.id!r},"
                        f"season_type={first_game.season.season_type.value!r}"
                    ),
                    "paired game rows must have opposite home/away flags",
                )
            )
        if first_game.game_date != second_game.game_date:
            issues.append(
                ValidationIssue(
                    "normalized_games",
                    (
                        f"game_id={first_game.game_id!r},season={first_game.season.id!r},"
                        f"season_type={first_game.season.season_type.value!r}"
                    ),
                    "paired game rows must have the same game date",
                )
            )
        if first_game.margin != -second_game.margin:
            issues.append(
                ValidationIssue(
                    "normalized_games",
                    (
                        f"game_id={first_game.game_id!r},season={first_game.season.id!r},"
                        f"season_type={first_game.season.season_type.value!r}"
                    ),
                    (
                        "paired game rows must have opposite margins: "
                        f"{first_game.team.team_id}={first_game.margin} "
                        f"{second_game.team.team_id}={second_game.margin}"
                    ),
                )
            )


def validate_team_history_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
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
            _validate_optional_non_negative_int(row["team_id"], "team_id")
            if row["team_id"] is None or row["team_id"] <= 0:
                raise ValueError("team_id must be a positive integer")
            _validate_required_text(row["abbreviation"], "abbreviation")
            _validate_required_text(row["franchise_id"], "franchise_id")
            _validate_required_text(row["lookup_abbreviation"], "lookup_abbreviation")

            season = Season.parse(row["season"], SeasonType.REGULAR.value)
            team = Team.from_id(row["team_id"])
            team_season = team.for_season(season)
            if team_season.abbreviation != row["abbreviation"]:
                raise ValueError("abbreviation does not match official team history")
            expected_lookup_abbreviation = team.current.abbreviation
            if row["lookup_abbreviation"] != expected_lookup_abbreviation:
                raise ValueError("lookup_abbreviation does not match canonical lookup abbreviation")
            expected_franchise_id = team.current.abbreviation.lower()
            if row["franchise_id"] != expected_franchise_id:
                raise ValueError("franchise_id does not match official team history")
        except (AssertionError, ValueError) as exc:
            issues.append(ValidationIssue("team_history", key, str(exc)))


def _build_normalized_game_record(row: sqlite3.Row) -> NormalizedGameRow:
    season = Season.parse(row["season"], row["season_type"])
    team = Team.from_id(row["team_id"])
    opponent_team = Team.from_id(row["opponent_team_id"])
    team.for_season(season)
    opponent_team.for_season(season)
    _validate_required_text(row["game_id"], "game_id")
    _validate_required_text(row["game_date"], "game_date")
    _validate_required_text(row["source"], "source")
    if not math.isfinite(row["margin"]):
        raise ValueError("margin must be finite")
    date.fromisoformat(row["game_date"])
    return NormalizedGameRow(
        game_id=row["game_id"],
        game_date=row["game_date"],
        season=season,
        team=team,
        opponent_team=opponent_team,
        is_home=bool(row["is_home"]),
        margin=row["margin"],
        source=row["source"],
    )


def _build_normalized_game_player_record(
    row: sqlite3.Row,
) -> tuple[NormalizedGamePlayerRow, Season]:
    season = Season.parse(row["season"], row["season_type"])
    team = Team.from_id(row["team_id"])
    _validate_required_text(row["game_id"], "game_id")
    return (
        NormalizedGamePlayerRow(
            game_id=row["game_id"],
            player=PlayerSummary(
                player_id=row["player_id"],
                player_name=row["player_name"],
            ),
            appeared=bool(row["appeared"]),
            minutes=row["minutes"],
            team=team,
        ),
        season,
    )


def _validate_normalized_scope_batch(
    *,
    team: Team,
    season: Season,
    games: list[NormalizedGameRow],
    game_players: list[NormalizedGamePlayerRow],
) -> None:
    game_keys: set[tuple[str, int]] = set()
    players_by_game_key: dict[tuple[str, int], list[NormalizedGamePlayerRow]] = defaultdict(list)

    for game in games:
        _validate_normalized_game_row(game, expected_team=team, expected_season=season)
        game_key = (game.game_id, game.team.team_id)
        if game_key in game_keys:
            raise ValueError(f"Duplicate canonical game row for {game_key!r}")
        game_keys.add(game_key)

    player_keys: set[tuple[str, int, int]] = set()
    for player in game_players:
        _validate_normalized_game_player_row(player, expected_team=team)
        player_key = (player.game_id, player.team.team_id, player.player.player_id)
        if player_key in player_keys:
            raise ValueError(f"Duplicate canonical player row for {player_key!r}")
        player_keys.add(player_key)
        players_by_game_key[(player.game_id, player.team.team_id)].append(player)

    if set(players_by_game_key) != game_keys:
        missing_players = sorted(game_keys - set(players_by_game_key))
        extra_players = sorted(set(players_by_game_key) - game_keys)
        raise ValueError(
            "Canonical game/player keys do not match: "
            f"missing_players={missing_players} extra_players={extra_players}"
        )

    for game_key, players in players_by_game_key.items():
        appeared_players = [player for player in players if player.appeared]
        if len(appeared_players) < 5:
            raise ValueError(
                f"Expected at least 5 appeared players for {game_key!r}; "
                f"found {len(appeared_players)}"
            )
        if len(players) > 25:
            raise ValueError(
                f"Expected at most 25 player rows for {game_key!r}; found {len(players)}"
            )
        total_minutes = sum(player.minutes or 0.0 for player in appeared_players)
        if total_minutes < 220.0 or total_minutes > 450.0:
            raise ValueError(
                f"Implausible total appeared minutes for {game_key!r}: {total_minutes}"
            )


def _validate_normalized_game_row(
    game: NormalizedGameRow,
    *,
    expected_team: Team,
    expected_season: Season,
) -> None:
    if not game.game_id.strip():
        raise ValueError("Canonical game_id must not be empty")
    if game.season != expected_season:
        raise ValueError(
            f"Canonical game {game.game_id!r} season {game.season!r}; expected {expected_season!r}"
        )
    if game.team != expected_team:
        raise ValueError(
            f"Canonical game {game.game_id!r} is not the same as {game.team!r}; "
            f"expected {expected_team!r}"
        )
    if game.opponent_team.team_id is None or game.opponent_team.team_id <= 0:
        raise ValueError(f"Canonical game {game.game_id!r} must have a positive opponent_team_id")
    if game.opponent_team.team_id == expected_team.team_id:
        raise ValueError(
            f"Canonical game {game.game_id!r} must not use the same team_id and opponent_team_id"
        )
    game.team.validate()
    game.opponent_team.validate()
    parsed_date = date.fromisoformat(game.game_date)
    if parsed_date.year not in {expected_season.start_year, expected_season.start_year + 1}:
        raise ValueError(
            f"Canonical game {game.game_id!r} date {game.game_date!r} "
            f"falls outside season {expected_season!r}"
        )
    if not math.isfinite(game.margin):
        raise ValueError(f"Canonical game {game.game_id!r} has non-finite margin")
    if not game.source.strip():
        raise ValueError(f"Canonical game {game.game_id!r} must have a non-empty source")


def _validate_normalized_game_player_row(
    player: NormalizedGamePlayerRow,
    *,
    expected_team: Team,
) -> None:
    player_ref = (
        "game "
        f"{player.game_id!r} player_id={player.player.player_id!r} "
        f"player_name={player.player.player_name!r}"
    )
    if not player.game_id.strip():
        raise ValueError("Canonical player game_id must not be empty")
    if player.team.team_id != expected_team.team_id:
        raise ValueError(
            f"Canonical player row for game {player.game_id!r} has team_id "
            f"{player.team.team_id!r}; expected {expected_team.team_id!r}"
        )
    if player.player.player_id <= 0:
        raise ValueError(f"Canonical player row for {player_ref} has invalid player_id")
    if not player.player.player_name.strip():
        raise ValueError(f"Canonical player row for {player_ref} must have a player name")

    minutes = player.minutes
    if minutes is None:
        return
    if not math.isfinite(minutes) or minutes < 0.0:
        raise ValueError(f"Canonical player row for {player_ref} has invalid minutes {minutes!r}")
    if minutes > 80.0:
        raise ValueError(
            f"Canonical player row for {player_ref} has implausible minutes {minutes!r}"
        )


__all__ = [
    "_validate_reciprocal_game_margins",
    "validate_normalized_cache_loads_table",
    "validate_normalized_cache_relations",
    "validate_normalized_game_players_table",
    "validate_normalized_games_table",
    "validate_team_history_table",
]
