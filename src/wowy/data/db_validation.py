from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from wowy.data.game_cache_db import NormalizedCacheLoadRow, initialize_game_cache_db
from wowy.data.player_metrics_db import (
    DEFAULT_PLAYER_METRICS_DB_PATH,
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
    _connect as _connect_player_metrics_db,
    _validate_iso_datetime,
    _validate_metric_full_span_rows,
    _validate_metric_rows,
    _validate_metric_scope_catalog_row,
    _validate_optional_non_negative_int,
    _validate_required_text,
)
from wowy.nba.models import CanonicalGamePlayerRecord, CanonicalGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.team_identity import resolve_team_id
from wowy.nba.ingest.validation import (
    _canonical_team_abbreviation,
    _validate_canonical_game,
    _validate_canonical_game_player,
    validate_normalized_cache_batch,
)


@dataclass(frozen=True)
class ValidationIssue:
    table: str
    key: str
    message: str


@dataclass(frozen=True)
class DatabaseValidationReport:
    issues: list[ValidationIssue]

    @property
    def ok(self) -> bool:
        return not self.issues

    @property
    def issue_count(self) -> int:
        return len(self.issues)


@dataclass(frozen=True)
class ValidationTrend:
    table: str
    signature: str
    count: int
    example_key: str
    example_message: str


@dataclass(frozen=True)
class DatabaseValidationSummary:
    issue_count: int
    table_counts: dict[str, int]
    trend_map: dict[str, dict[str, int]]
    trends: list[ValidationTrend]

    @property
    def ok(self) -> bool:
        return self.issue_count == 0

    def to_dict(self) -> dict[str, object]:
        return {
            "issue_count": self.issue_count,
            "ok": self.ok,
            "table_counts": self.table_counts,
            "trend_map": self.trend_map,
            "trends": [
                {
                    "table": trend.table,
                    "signature": trend.signature,
                    "count": trend.count,
                    "example_key": trend.example_key,
                    "example_message": trend.example_message,
                }
                for trend in self.trends
            ],
        }


_QUOTED_VALUE_PATTERN = re.compile(r"'[^']*'")
_NUMBER_PATTERN = re.compile(r"(?<![A-Za-z])-?\d+(?:\.\d+)?")
ValidationProgressFn = Callable[[int, int, str], None]


def audit_player_metrics_db(
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    progress: ValidationProgressFn | None = None,
) -> DatabaseValidationReport:
    initialize_game_cache_db(db_path)
    issues: list[ValidationIssue] = []
    steps = (
        ("normalized games", _validate_normalized_games_table),
        ("normalized game players", _validate_normalized_game_players_table),
        ("normalized cache loads", _validate_normalized_cache_loads_table),
        ("normalized cache relations", _validate_normalized_cache_relations),
        ("metric player season values", _validate_metric_player_season_values_table),
        ("metric scope catalog", _validate_metric_scope_catalog_table),
        ("metric full span tables", _validate_metric_full_span_tables),
    )
    total_steps = len(steps) + 1
    current_step = 0

    def report_progress(label: str) -> None:
        if progress is not None:
            progress(current_step, total_steps, label)

    with _connect_player_metrics_db(db_path) as connection:
        current_step = 1
        report_progress("Validating normalized games")
        _validate_normalized_games_table(connection, issues)
        current_step = 2
        report_progress("Validating normalized game players")
        _validate_normalized_game_players_table(connection, issues)
        current_step = 3
        report_progress("Validating normalized cache loads")
        _validate_normalized_cache_loads_table(connection, issues)
        current_step = 4
        report_progress("Validating normalized cache relations")
        _validate_normalized_cache_relations(connection, issues)
        current_step = 5
        report_progress("Validating metric player season values")
        metric_row_groups, metadata_rows = _validate_metric_player_season_values_table(
            connection,
            issues,
        )
        current_step = 6
        report_progress("Validating metric scope catalog")
        catalog_rows = _validate_metric_scope_catalog_table(connection, issues)
        current_step = 7
        report_progress("Validating metric full span tables")
        full_span_groups = _validate_metric_full_span_tables(connection, issues)
        current_step = 8
        report_progress("Validating metric store relations")
        _validate_metric_store_relations(
            connection=connection,
            metric_row_groups=metric_row_groups,
            metadata_rows=metadata_rows,
            catalog_rows=catalog_rows,
            full_span_groups=full_span_groups,
            issues=issues,
        )

    return DatabaseValidationReport(issues=issues)


def assert_valid_player_metrics_db(
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
) -> None:
    report = audit_player_metrics_db(db_path)
    if report.ok:
        return
    preview = "; ".join(
        f"{issue.table}[{issue.key}]: {issue.message}" for issue in report.issues[:5]
    )
    if report.issue_count > 5:
        preview += f"; ... {report.issue_count - 5} more"
    raise ValueError(f"Database validation failed with {report.issue_count} issue(s): {preview}")


def summarize_validation_report(
    report: DatabaseValidationReport,
) -> DatabaseValidationSummary:
    table_counts = Counter(issue.table for issue in report.issues)
    trend_counts: Counter[tuple[str, str]] = Counter()
    trend_examples: dict[tuple[str, str], ValidationIssue] = {}

    for issue in report.issues:
        signature = normalize_issue_message(issue.message)
        key = (issue.table, signature)
        trend_counts[key] += 1
        trend_examples.setdefault(key, issue)

    trends = [
        ValidationTrend(
            table=table,
            signature=signature,
            count=count,
            example_key=trend_examples[(table, signature)].key,
            example_message=trend_examples[(table, signature)].message,
        )
        for (table, signature), count in sorted(
            trend_counts.items(),
            key=lambda item: (-item[1], item[0][0], item[0][1]),
        )
    ]
    trend_map: dict[str, dict[str, int]] = defaultdict(dict)
    for trend in trends:
        trend_map[trend.table][trend.signature] = trend.count

    return DatabaseValidationSummary(
        issue_count=report.issue_count,
        table_counts=dict(sorted(table_counts.items())),
        trend_map=dict(sorted(trend_map.items())),
        trends=trends,
    )


def render_validation_summary(
    summary: DatabaseValidationSummary,
    *,
    top_n: int = 10,
) -> str:
    lines = [
        f"Database validation status: {'ok' if summary.ok else 'invalid'}",
        f"Total issues: {summary.issue_count}",
    ]
    if summary.issue_count == 0:
        return "\n".join(lines)

    lines.append("")
    lines.append("Issues by table:")
    for table, count in sorted(summary.table_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- {table}: {count}")

    lines.append("")
    lines.append(f"Top {min(top_n, len(summary.trends))} error trends:")
    for index, trend in enumerate(summary.trends[:top_n], start=1):
        lines.append(f"{index}. {trend.table} x{trend.count}")
        lines.append(f"   signature: {trend.signature}")
        lines.append(f"   example key: {trend.example_key}")
        lines.append(f"   example: {trend.example_message}")

    return "\n".join(lines)


def normalize_issue_message(message: str) -> str:
    normalized = _QUOTED_VALUE_PATTERN.sub("'<value>'", message)
    normalized = _NUMBER_PATTERN.sub("<num>", normalized)
    return " ".join(normalized.split())


def _validate_normalized_games_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    rows = connection.execute(
        """
        SELECT
            game_id,
            season,
            game_date,
            team,
            team_id,
            opponent,
            opponent_team_id,
            is_home,
            margin,
            season_type,
            source
        FROM normalized_games
        ORDER BY season_type, season, team, game_id
        """
    ).fetchall()
    for row in rows:
        key = (
            f"game_id={row['game_id']!r},team={row['team']!r},season={row['season']!r},"
            f"season_type={row['season_type']!r}"
        )
        if row["is_home"] not in {0, 1}:
            issues.append(
                ValidationIssue(
                    table="normalized_games",
                    key=key,
                    message=f"is_home must be 0 or 1, got {row['is_home']!r}",
                )
            )
            continue
        game = CanonicalGameRecord(
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
                expected_team_id=game.team_id or 0,
                expected_season=game.season,
                expected_season_type=game.season_type,
            )
        except ValueError as exc:
            issues.append(ValidationIssue(table="normalized_games", key=key, message=str(exc)))


def _validate_normalized_game_players_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    rows = connection.execute(
        """
        SELECT
            game_id,
            season,
            season_type,
            team,
            team_id,
            player_id,
            player_name,
            appeared,
            minutes
        FROM normalized_game_players
        ORDER BY season_type, season, team, game_id, player_id
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
            issues.append(
                ValidationIssue(
                    table="normalized_game_players",
                    key=key,
                    message=str(exc),
                )
            )
            continue
        if row["appeared"] not in {0, 1}:
            issues.append(
                ValidationIssue(
                    table="normalized_game_players",
                    key=key,
                    message=f"appeared must be 0 or 1, got {row['appeared']!r}",
                )
            )
            continue
        player = CanonicalGamePlayerRecord(
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
            issues.append(
                ValidationIssue(
                    table="normalized_game_players",
                    key=key,
                    message=str(exc),
                )
            )


def _validate_normalized_cache_loads_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    rows = connection.execute(
        """
        SELECT
            team,
            team_id,
            season,
            season_type,
            source_path,
            source_snapshot,
            source_kind,
            build_version,
            refreshed_at,
            games_row_count,
            game_players_row_count
        FROM normalized_cache_loads
        ORDER BY season_type, season, team_id
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
            if resolve_team_id(load_row.team) != load_row.team_id:
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
            issues.append(
                ValidationIssue(
                    table="normalized_cache_loads",
                    key=key,
                    message=str(exc),
                )
            )


def _validate_normalized_cache_relations(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    game_rows = connection.execute(
        """
        SELECT
            game_id,
            season,
            game_date,
            team,
            team_id,
            opponent,
            opponent_team_id,
            is_home,
            margin,
            season_type,
            source
        FROM normalized_games
        ORDER BY season_type, season, team, game_id
        """
    ).fetchall()
    player_rows = connection.execute(
        """
        SELECT
            game_id,
            season,
            season_type,
            team,
            team_id,
            player_id,
            player_name,
            appeared,
            minutes
        FROM normalized_game_players
        ORDER BY season_type, season, team, game_id, player_id
        """
    ).fetchall()
    load_rows = connection.execute(
        """
        SELECT
            team,
            team_id,
            season,
            season_type,
            source_path,
            source_snapshot,
            source_kind,
            build_version,
            refreshed_at,
            games_row_count,
            game_players_row_count
        FROM normalized_cache_loads
        """
    ).fetchall()

    games_by_scope: dict[tuple[int, str, str], list[CanonicalGameRecord]] = defaultdict(list)
    players_by_scope: dict[tuple[int, str, str], list[CanonicalGamePlayerRecord]] = defaultdict(
        list
    )
    game_key_scope_map: dict[tuple[str, int], tuple[int, str, str]] = {}

    for row in game_rows:
        game = CanonicalGameRecord(
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
        scope = (game.team_id or 0, game.season, game.season_type)
        games_by_scope[scope].append(game)
        game_key_scope_map[(game.game_id, game.team_id or 0)] = scope

    for row in player_rows:
        player = CanonicalGamePlayerRecord(
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
        game_scope = game_key_scope_map.get((player.game_id, player.team_id or 0))
        if game_scope is None:
            issues.append(
                ValidationIssue(
                    table="normalized_game_players",
                    key=(
                        f"game_id={player.game_id!r},team={player.team!r},"
                        f"player_id={player.player_id!r}"
                    ),
                    message="player row has no matching normalized_games row",
                )
            )
        elif game_scope != scope:
            issues.append(
                ValidationIssue(
                    table="normalized_game_players",
                    key=(
                        f"game_id={player.game_id!r},team={player.team!r},"
                        f"player_id={player.player_id!r}"
                    ),
                    message="player row season or season_type does not match normalized_games row",
                )
            )

    _validate_reciprocal_game_margins(game_rows, issues)

    game_scopes = set(games_by_scope)
    player_scopes = set(players_by_scope)
    load_scopes = {(row["team_id"], row["season"], row["season_type"]) for row in load_rows}

    for scope in sorted(game_scopes | player_scopes):
        team_id, season, season_type = scope
        games_for_scope = games_by_scope.get(scope, [])
        players_for_scope = players_by_scope.get(scope, [])
        team = (
            games_for_scope[0].team
            if games_for_scope
            else players_for_scope[0].team
        )
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
                ValidationIssue(
                    table="normalized_cache_relations",
                    key=f"team={team!r},season={season!r},season_type={season_type!r}",
                    message=str(exc),
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
                    table="normalized_cache_loads",
                    key=key,
                    message=(
                        "games_row_count does not match normalized_games count: "
                        f"{row['games_row_count']} != {game_count}"
                    ),
                )
            )
        if row["game_players_row_count"] != player_count:
            issues.append(
                ValidationIssue(
                    table="normalized_cache_loads",
                    key=key,
                    message=(
                        "game_players_row_count does not match normalized_game_players count: "
                        f"{row['game_players_row_count']} != {player_count}"
                    ),
                )
            )

    for scope in sorted((game_scopes | player_scopes) - load_scopes):
        issues.append(
            ValidationIssue(
                table="normalized_cache_loads",
                key=f"team={scope[0]!r},season={scope[1]!r},season_type={scope[2]!r}",
                message="missing normalized_cache_loads row for existing normalized cache scope",
            )
        )


def _validate_reciprocal_game_margins(
    game_rows: list[sqlite3.Row],
    issues: list[ValidationIssue],
) -> None:
    games_by_id: dict[tuple[str, str, str], list[CanonicalGameRecord]] = defaultdict(list)

    for row in game_rows:
        games_by_id[(row["season"], row["season_type"], row["game_id"])].append(
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
        )

    for key, games in games_by_id.items():
        if len(games) != 2:
            continue

        first_game, second_game = games
        if (
            first_game.identity_team != second_game.identity_opponent
            or second_game.identity_team != first_game.identity_opponent
        ):
            issues.append(
                ValidationIssue(
                    table="normalized_games",
                    key=(
                        f"game_id={first_game.game_id!r},season={first_game.season!r},"
                        f"season_type={first_game.season_type!r}"
                    ),
                    message="paired game rows must reference each other as opponents",
                )
            )
        if first_game.is_home == second_game.is_home:
            issues.append(
                ValidationIssue(
                    table="normalized_games",
                    key=(
                        f"game_id={first_game.game_id!r},season={first_game.season!r},"
                        f"season_type={first_game.season_type!r}"
                    ),
                    message="paired game rows must have opposite home/away flags",
                )
            )
        if first_game.game_date != second_game.game_date:
            issues.append(
                ValidationIssue(
                    table="normalized_games",
                    key=(
                        f"game_id={first_game.game_id!r},season={first_game.season!r},"
                        f"season_type={first_game.season_type!r}"
                    ),
                    message="paired game rows must have the same game date",
                )
            )
        if first_game.margin != -second_game.margin:
            issues.append(
                ValidationIssue(
                    table="normalized_games",
                    key=(
                        f"game_id={first_game.game_id!r},season={first_game.season!r},"
                        f"season_type={first_game.season_type!r}"
                    ),
                    message=(
                        "paired game rows must have opposite margins: "
                        f"{first_game.team}={first_game.margin} "
                        f"{second_game.team}={second_game.margin}"
                    ),
                )
            )


def _validate_metric_player_season_values_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> tuple[
    dict[tuple[str, str], list[PlayerSeasonMetricRow]],
    dict[tuple[str, str], tuple[str, str, str, int]],
]:
    metadata_rows = connection.execute(
        """
        SELECT
            metric,
            scope_key,
            metric_label,
            build_version,
            source_fingerprint,
            row_count
        FROM metric_store_metadata_v2
        """
    ).fetchall()
    metadata_by_key = {
        (row["metric"], row["scope_key"]): (
            row["metric_label"],
            row["build_version"],
            row["source_fingerprint"],
            row["row_count"],
        )
        for row in metadata_rows
    }

    rows = connection.execute(
        """
        SELECT
            metric,
            metric_label,
            scope_key,
            team_filter,
            season_type,
            season,
            player_id,
            player_name,
            value,
            sample_size,
            secondary_sample_size,
            average_minutes,
            total_minutes,
            details_json
        FROM metric_player_season_values
        ORDER BY metric, scope_key, season, player_id
        """
    ).fetchall()

    groups: dict[tuple[str, str], list[PlayerSeasonMetricRow]] = defaultdict(list)
    for row in rows:
        groups[(row["metric"], row["scope_key"])].append(
            PlayerSeasonMetricRow(
                metric=row["metric"],
                metric_label=row["metric_label"],
                scope_key=row["scope_key"],
                team_filter=row["team_filter"],
                season_type=row["season_type"],
                season=row["season"],
                player_id=row["player_id"],
                player_name=row["player_name"],
                value=row["value"],
                sample_size=row["sample_size"],
                secondary_sample_size=row["secondary_sample_size"],
                average_minutes=row["average_minutes"],
                total_minutes=row["total_minutes"],
                details=json.loads(row["details_json"]),
            )
        )

    for key, group_rows in groups.items():
        metric, scope_key = key
        metric_label, build_version, source_fingerprint, _row_count = metadata_by_key.get(
            key,
            (group_rows[0].metric_label, "missing-metadata", "missing-metadata", -1),
        )
        try:
            _validate_metric_rows(
                metric=metric,
                scope_key=scope_key,
                metric_label=metric_label,
                build_version=build_version,
                source_fingerprint=source_fingerprint,
                rows=group_rows,
            )
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    table="metric_player_season_values",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message=str(exc),
                )
            )

    return groups, metadata_by_key


def _validate_metric_scope_catalog_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> dict[tuple[str, str], MetricScopeCatalogRow]:
    rows = connection.execute(
        """
        SELECT
            metric,
            scope_key,
            metric_label,
            team_filter,
            season_type,
            available_seasons_json,
            available_teams_json,
            full_span_start_season,
            full_span_end_season,
            updated_at
        FROM metric_scope_catalog
        ORDER BY metric, scope_key
        """
    ).fetchall()
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow] = {}
    for row in rows:
        catalog_row = MetricScopeCatalogRow(
            metric=row["metric"],
            scope_key=row["scope_key"],
            metric_label=row["metric_label"],
            team_filter=row["team_filter"],
            season_type=row["season_type"],
            available_seasons=json.loads(row["available_seasons_json"]),
            available_teams=json.loads(row["available_teams_json"]),
            full_span_start_season=row["full_span_start_season"],
            full_span_end_season=row["full_span_end_season"],
            updated_at=row["updated_at"],
        )
        key = (catalog_row.metric, catalog_row.scope_key)
        catalog_rows[key] = catalog_row
        try:
            _validate_metric_scope_catalog_row(catalog_row)
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    table="metric_scope_catalog",
                    key=f"metric={catalog_row.metric!r},scope_key={catalog_row.scope_key!r}",
                    message=str(exc),
                )
            )
    return catalog_rows


def _validate_metric_full_span_tables(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> dict[tuple[str, str], tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]]:
    series_rows = connection.execute(
        """
        SELECT
            metric,
            scope_key,
            player_id,
            player_name,
            span_average_value,
            season_count,
            rank_order
        FROM metric_full_span_series
        ORDER BY metric, scope_key, rank_order, player_id
        """
    ).fetchall()
    point_rows = connection.execute(
        """
        SELECT
            metric,
            scope_key,
            player_id,
            season,
            value
        FROM metric_full_span_points
        ORDER BY metric, scope_key, player_id, season
        """
    ).fetchall()

    series_groups: dict[tuple[str, str], list[MetricFullSpanSeriesRow]] = defaultdict(list)
    point_groups: dict[tuple[str, str], list[MetricFullSpanPointRow]] = defaultdict(list)

    for row in series_rows:
        series_groups[(row["metric"], row["scope_key"])].append(
            MetricFullSpanSeriesRow(
                metric=row["metric"],
                scope_key=row["scope_key"],
                player_id=row["player_id"],
                player_name=row["player_name"],
                span_average_value=row["span_average_value"],
                season_count=row["season_count"],
                rank_order=row["rank_order"],
            )
        )
    for row in point_rows:
        point_groups[(row["metric"], row["scope_key"])].append(
            MetricFullSpanPointRow(
                metric=row["metric"],
                scope_key=row["scope_key"],
                player_id=row["player_id"],
                season=row["season"],
                value=row["value"],
            )
        )

    groups: dict[
        tuple[str, str], tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]
    ] = {}
    for key in sorted(set(series_groups) | set(point_groups)):
        series = series_groups.get(key, [])
        points = point_groups.get(key, [])
        groups[key] = (series, points)
        try:
            _validate_metric_full_span_rows(
                metric=key[0],
                scope_key=key[1],
                series_rows=series,
                point_rows=points,
            )
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    table="metric_full_span",
                    key=f"metric={key[0]!r},scope_key={key[1]!r}",
                    message=str(exc),
                )
            )

    return groups


def _validate_metric_store_relations(
    *,
    connection: sqlite3.Connection,
    metric_row_groups: dict[tuple[str, str], list[PlayerSeasonMetricRow]],
    metadata_rows: dict[tuple[str, str], tuple[str, str, str, int]],
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow],
    full_span_groups: dict[
        tuple[str, str], tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]
    ],
    issues: list[ValidationIssue],
) -> None:
    metric_scopes = set(metric_row_groups)
    metadata_scopes = set(metadata_rows)
    catalog_scopes = set(catalog_rows)
    full_span_scopes = set(full_span_groups)
    cache_load_counts, fingerprint_by_season_type = _load_normalized_cache_state(connection)

    for key, rows in metric_row_groups.items():
        metric, scope_key = key
        seasons = sorted({row.season for row in rows})
        season_set = set(seasons)
        metadata_row = metadata_rows.get(key)
        if metadata_row is None:
            issues.append(
                ValidationIssue(
                    table="metric_store_metadata_v2",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message="missing metadata row for metric scope",
                )
            )
        elif metadata_row[3] != len(rows):
            issues.append(
                ValidationIssue(
                    table="metric_store_metadata_v2",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message=f"row_count does not match metric rows: {metadata_row[3]} != {len(rows)}",
                )
            )
        catalog_row = catalog_rows.get(key)
        if catalog_row is None:
            issues.append(
                ValidationIssue(
                    table="metric_scope_catalog",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message="missing catalog row for metric scope",
                )
            )
        elif not season_set.issubset(set(catalog_row.available_seasons)):
            issues.append(
                ValidationIssue(
                    table="metric_scope_catalog",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message=(
                        "available_seasons is missing seasons present in metric rows: "
                        f"catalog={catalog_row.available_seasons!r} metric_rows={seasons!r}"
                    ),
                )
            )

    all_scopes = metric_scopes | metadata_scopes | catalog_scopes | full_span_scopes
    for key in sorted(all_scopes):
        metric, scope_key = key
        catalog_row = catalog_rows.get(key)
        group_rows = metric_row_groups.get(key, [])
        season_type = (
            catalog_row.season_type
            if catalog_row is not None
            else group_rows[0].season_type if group_rows else None
        )
        if season_type is None:
            continue
        if cache_load_counts.get(season_type, 0) == 0:
            issues.append(
                ValidationIssue(
                    table="metric_store_metadata_v2",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message=(
                        "derived metric scope exists but normalized cache is empty for "
                        f"season_type {season_type!r}"
                    ),
                )
            )
            continue
        metadata_row = metadata_rows.get(key)
        if metadata_row is None:
            continue
        current_fingerprint = fingerprint_by_season_type.get(season_type)
        if current_fingerprint is None:
            continue
        if metadata_row[2] != current_fingerprint:
            issues.append(
                ValidationIssue(
                    table="metric_store_metadata_v2",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message=(
                        "source_fingerprint does not match normalized cache for "
                        f"season_type {season_type!r}"
                    ),
                )
            )

    for key in sorted(metadata_scopes - metric_scopes):
        issues.append(
            ValidationIssue(
                table="metric_store_metadata_v2",
                key=f"metric={key[0]!r},scope_key={key[1]!r}",
                message="metadata row has no matching metric rows",
            )
        )

    for key in sorted(catalog_scopes - metric_scopes):
        issues.append(
            ValidationIssue(
                table="metric_scope_catalog",
                key=f"metric={key[0]!r},scope_key={key[1]!r}",
                message="catalog row has no matching metric rows",
            )
        )

    for key, (series_rows, point_rows) in full_span_groups.items():
        metric, scope_key = key
        if key not in catalog_rows:
            issues.append(
                ValidationIssue(
                    table="metric_full_span",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message="full-span rows have no matching catalog row",
                )
            )
            continue
        catalog_row = catalog_rows[key]
        allowed_seasons = set(catalog_row.available_seasons)
        for point_row in point_rows:
            if point_row.season not in allowed_seasons:
                issues.append(
                    ValidationIssue(
                        table="metric_full_span_points",
                        key=(
                            f"metric={metric!r},scope_key={scope_key!r},"
                            f"player_id={point_row.player_id!r},season={point_row.season!r}"
                        ),
                        message="point season is not present in catalog available_seasons",
                    )
                )
        if key in metric_row_groups and not series_rows and metric_row_groups[key]:
            issues.append(
                ValidationIssue(
                    table="metric_full_span_series",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message="metric rows exist but no full-span series rows were stored",
                )
            )

    for key in sorted(metric_scopes - full_span_scopes):
        issues.append(
            ValidationIssue(
                table="metric_full_span",
                key=f"metric={key[0]!r},scope_key={key[1]!r}",
                message="metric scope has no matching full-span rows",
            )
        )


def _load_normalized_cache_state(
    connection: sqlite3.Connection,
) -> tuple[dict[str, int], dict[str, str]]:
    rows = connection.execute(
        """
        SELECT
            team,
            team_id,
            season,
            season_type,
            source_path,
            source_snapshot,
            source_kind,
            build_version,
            games_row_count,
            game_players_row_count,
            expected_games_row_count,
            skipped_games_row_count
        FROM normalized_cache_loads
        ORDER BY season_type, season, team_id
        """
    ).fetchall()
    counts: dict[str, int] = Counter()
    digests: dict[str, object] = {}
    for row in rows:
        season_type = row["season_type"]
        counts[season_type] += 1
        digest = digests.setdefault(season_type, hashlib.sha256())
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
    return (
        counts,
        {season_type: digest.hexdigest() for season_type, digest in digests.items()},
    )
