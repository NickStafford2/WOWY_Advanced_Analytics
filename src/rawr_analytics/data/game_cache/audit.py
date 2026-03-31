from __future__ import annotations

import sqlite3

from rawr_analytics.data._validation_issue import ValidationIssue
from rawr_analytics.data.game_cache._validation import (
    _validate_normalized_cache_loads_table as _validate_normalized_cache_loads_table_impl,
)
from rawr_analytics.data.game_cache._validation import (
    _validate_normalized_cache_relations as _validate_normalized_cache_relations_impl,
)
from rawr_analytics.data.game_cache._validation import (
    _validate_normalized_game_players_table as _validate_normalized_game_players_table_impl,
)
from rawr_analytics.data.game_cache._validation import (
    _validate_normalized_games_table as _validate_normalized_games_table_impl,
)
from rawr_analytics.data.game_cache._validation import (
    _validate_team_history_table as _validate_team_history_table_impl,
)


def _audit_game_cache_tables(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    audit_team_history_table(connection, issues)
    audit_normalized_games_table(connection, issues)
    audit_normalized_game_players_table(connection, issues)
    audit_normalized_cache_loads_table(connection, issues)
    audit_normalized_cache_relations(connection, issues)


def audit_team_history_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    _validate_team_history_table_impl(
        connection,
        issues,
    )


def audit_normalized_games_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    _validate_normalized_games_table_impl(
        connection,
        issues,
    )


def audit_normalized_game_players_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    _validate_normalized_game_players_table_impl(
        connection,
        issues,
    )


def audit_normalized_cache_loads_table(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    _validate_normalized_cache_loads_table_impl(
        connection,
        issues,
    )


def audit_normalized_cache_relations(
    connection: sqlite3.Connection,
    issues: list[ValidationIssue],
) -> None:
    _validate_normalized_cache_relations_impl(
        connection,
        issues,
    )
