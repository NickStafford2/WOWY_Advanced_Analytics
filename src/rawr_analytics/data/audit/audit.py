from __future__ import annotations

import sqlite3
from collections.abc import Callable

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH, NORMALIZED_CACHE_DB_PATH
from rawr_analytics.data._validation import ValidationIssue
from rawr_analytics.data.audit._relations import validate_metric_store_relations
from rawr_analytics.data.audit.reporting import DatabaseValidationReport
from rawr_analytics.data.game_cache._schema import initialize_game_cache_db
from rawr_analytics.data.game_cache._validation import (
    validate_normalized_cache_loads_table,
    validate_normalized_cache_relations,
    validate_normalized_game_players_table,
    validate_normalized_games_table,
)
from rawr_analytics.data.metric_store.audit import audit_metric_store_tables
from rawr_analytics.data.metric_store.schema import initialize_metric_store_db

ValidationProgressFn = Callable[[int, int, str], None]


def audit_player_metrics_db(
    progress: ValidationProgressFn | None = None,
) -> DatabaseValidationReport:
    initialize_game_cache_db()
    issues: list[ValidationIssue] = []
    steps = (
        "normalized games",
        "normalized game players",
        "normalized cache loads",
        "normalized cache relations",
        "metric player season values",
        "metric cache catalog",
        "metric store relations",
    )
    total_steps = len(steps)
    current_step = 0

    def report_progress(label: str) -> None:
        if progress is not None:
            progress(current_step, total_steps, label)

    with sqlite3.connect(NORMALIZED_CACHE_DB_PATH) as cache_connection:
        cache_connection.row_factory = sqlite3.Row

        current_step = 1
        report_progress("Validating normalized games")
        validate_normalized_games_table(cache_connection, issues)

        current_step = 2
        report_progress("Validating normalized game players")
        validate_normalized_game_players_table(cache_connection, issues)

        current_step = 3
        report_progress("Validating normalized cache loads")
        validate_normalized_cache_loads_table(cache_connection, issues)

        current_step = 4
        report_progress("Validating normalized cache relations")
        validate_normalized_cache_relations(cache_connection, issues)

    initialize_metric_store_db()
    with sqlite3.connect(METRIC_STORE_DB_PATH) as metric_connection:
        metric_connection.row_factory = sqlite3.Row

        current_step = 5
        report_progress("Validating metric player season values")
        metric_audit_state = audit_metric_store_tables(
            metric_connection,
            issues,
        )

        current_step = 6
        report_progress("Validating metric cache catalog")

        current_step = 7
        report_progress("Validating metric store relations")
        validate_metric_store_relations(
            rawr_row_groups=metric_audit_state.rawr_row_groups,
            wowy_row_groups=metric_audit_state.wowy_row_groups,
            metadata_rows=metric_audit_state.metadata_rows,
            catalog_rows=metric_audit_state.catalog_rows,
            catalog_season_rows=metric_audit_state.catalog_season_rows,
            issues=issues,
        )

    return DatabaseValidationReport(issues=issues)


def assert_valid_player_metrics_db() -> None:
    report = audit_player_metrics_db()
    if report.ok:
        return

    preview = "; ".join(
        f"{issue.table}[{issue.key}]: {issue.message}" for issue in report.issues[:5]
    )
    if report.issue_count > 5:
        preview += f"; ... {report.issue_count - 5} more"

    raise ValueError(f"Database validation failed with {report.issue_count} issue(s): {preview}")
