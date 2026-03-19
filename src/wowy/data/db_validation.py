from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

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
from wowy.nba.models import NormalizedGamePlayerRecord, NormalizedGameRecord
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.validation import (
    _canonical_team_abbreviation,
    _validate_normalized_game,
    _validate_normalized_game_player,
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


def audit_player_metrics_db(
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
) -> DatabaseValidationReport:
    initialize_game_cache_db(db_path)
    issues: list[ValidationIssue] = []

    with _connect_player_metrics_db(db_path) as connection:
        _validate_normalized_games_table(connection, issues)
        _validate_normalized_game_players_table(connection, issues)
        _validate_normalized_cache_loads_table(connection, issues)
        _validate_normalized_cache_relations(connection, issues)
        metric_row_groups, metadata_rows = _validate_metric_player_season_values_table(
            connection,
            issues,
        )
        catalog_rows = _validate_metric_scope_catalog_table(connection, issues)
        full_span_groups = _validate_metric_full_span_tables(connection, issues)
        _validate_metric_store_relations(
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
            opponent,
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
        )
        try:
            _validate_normalized_game(
                game,
                expected_team=game.team,
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
        player = NormalizedGamePlayerRecord(
            game_id=row["game_id"],
            team=row["team"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            appeared=bool(row["appeared"]),
            minutes=row["minutes"],
        )
        try:
            _validate_normalized_game_player(player, expected_team=row["team"])
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
        ORDER BY season_type, season, team
        """
    ).fetchall()
    for row in rows:
        key = f"team={row['team']!r},season={row['season']!r},season_type={row['season_type']!r}"
        load_row = NormalizedCacheLoadRow(
            team=row["team"],
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
            opponent,
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

    games_by_scope: dict[tuple[str, str, str], list[NormalizedGameRecord]] = defaultdict(list)
    players_by_scope: dict[tuple[str, str, str], list[NormalizedGamePlayerRecord]] = defaultdict(
        list
    )
    game_key_scope_map: dict[tuple[str, str], tuple[str, str, str]] = {}

    for row in game_rows:
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
        )
        scope = (game.team, game.season, game.season_type)
        games_by_scope[scope].append(game)
        game_key_scope_map[(game.game_id, game.team)] = scope

    for row in player_rows:
        player = NormalizedGamePlayerRecord(
            game_id=row["game_id"],
            team=row["team"],
            player_id=row["player_id"],
            player_name=row["player_name"],
            appeared=bool(row["appeared"]),
            minutes=row["minutes"],
        )
        scope = (row["team"], row["season"], row["season_type"])
        players_by_scope[scope].append(player)
        game_scope = game_key_scope_map.get((player.game_id, player.team))
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

    game_scopes = set(games_by_scope)
    player_scopes = set(players_by_scope)
    load_scopes = {(row["team"], row["season"], row["season_type"]) for row in load_rows}

    for scope in sorted(game_scopes | player_scopes):
        team, season, season_type = scope
        try:
            validate_normalized_cache_batch(
                team=team,
                season=season,
                season_type=season_type,
                games=games_by_scope.get(scope, []),
                game_players=players_by_scope.get(scope, []),
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
        scope = (row["team"], row["season"], row["season_type"])
        game_count = len(games_by_scope.get(scope, []))
        player_count = len(players_by_scope.get(scope, []))
        key = f"team={scope[0]!r},season={scope[1]!r},season_type={scope[2]!r}"
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
