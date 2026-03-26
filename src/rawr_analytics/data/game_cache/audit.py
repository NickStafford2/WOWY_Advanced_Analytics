from __future__ import annotations

import sqlite3
from typing import Callable, TypeVar

from rawr_analytics.data.game_cache.validation import (
    _validate_normalized_cache_loads_table as _validate_normalized_cache_loads_table_impl,
)
from rawr_analytics.data.game_cache.validation import (
    _validate_normalized_cache_relations as _validate_normalized_cache_relations_impl,
)
from rawr_analytics.data.game_cache.validation import (
    _validate_normalized_game_players_table as _validate_normalized_game_players_table_impl,
)
from rawr_analytics.data.game_cache.validation import (
    _validate_normalized_games_table as _validate_normalized_games_table_impl,
)
from rawr_analytics.data.game_cache.validation import (
    _validate_team_history_table as _validate_team_history_table_impl,
)

IssueT = TypeVar("IssueT")
IssueFactory = Callable[[str, str, str], IssueT]


def audit_game_cache_tables(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    audit_team_history_table(connection, issues, issue_factory=issue_factory)
    audit_normalized_games_table(connection, issues, issue_factory=issue_factory)
    audit_normalized_game_players_table(connection, issues, issue_factory=issue_factory)
    audit_normalized_cache_loads_table(connection, issues, issue_factory=issue_factory)
    audit_normalized_cache_relations(connection, issues, issue_factory=issue_factory)


def audit_team_history_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    _validate_team_history_table_impl(
        connection,
        issues,
        issue_factory=issue_factory,
    )


def audit_normalized_games_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    _validate_normalized_games_table_impl(
        connection,
        issues,
        issue_factory=issue_factory,
    )


def audit_normalized_game_players_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    _validate_normalized_game_players_table_impl(
        connection,
        issues,
        issue_factory=issue_factory,
    )


def audit_normalized_cache_loads_table(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    _validate_normalized_cache_loads_table_impl(
        connection,
        issues,
        issue_factory=issue_factory,
    )


def audit_normalized_cache_relations(
    connection: sqlite3.Connection,
    issues: list[IssueT],
    *,
    issue_factory: IssueFactory[IssueT],
) -> None:
    _validate_normalized_cache_relations_impl(
        connection,
        issues,
        issue_factory=issue_factory,
    )
