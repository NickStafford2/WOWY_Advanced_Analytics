from __future__ import annotations

import re
import sqlite3
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data._paths import METRIC_STORE_DB_PATH, NORMALIZED_CACHE_DB_PATH
from rawr_analytics.data._validation_issue import ValidationIssue
from rawr_analytics.data.game_cache.store import load_cache_snapshot
from rawr_analytics.data.game_cache._schema import initialize_game_cache_db
from rawr_analytics.data.game_cache._validation import (
    validate_normalized_cache_loads_table,
    validate_normalized_cache_relations,
    validate_normalized_game_players_table,
    validate_normalized_games_table,
)
from rawr_analytics.data.metric_store import (
    audit_metric_store_tables,
    initialize_player_metrics_db,
)
from rawr_analytics.data.metric_store._catalog import MetricScopeCatalogRow
from rawr_analytics.data.metric_store.audit import MetricStoreAuditMetadata
from rawr_analytics.data.metric_store.full_span import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
)
from rawr_analytics.data.metric_store.rawr import RawrPlayerSeasonValueRow
from rawr_analytics.data.metric_store.wowy import WowyPlayerSeasonValueRow
from rawr_analytics.shared.season import SeasonType


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
        "metric scope catalog",
        "metric full span tables",
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

    initialize_player_metrics_db()
    with sqlite3.connect(METRIC_STORE_DB_PATH) as metric_connection:
        metric_connection.row_factory = sqlite3.Row
        current_step = 5
        report_progress("Validating metric player season values")
        metric_audit_state = audit_metric_store_tables(
            metric_connection,
            issues,
        )
        current_step = 6
        report_progress("Validating metric scope catalog")
        current_step = 7
        report_progress("Validating metric full span tables")
        current_step = 8
        report_progress("Validating metric store relations")
        _validate_metric_store_relations(
            rawr_row_groups=metric_audit_state.rawr_row_groups,
            wowy_row_groups=metric_audit_state.wowy_row_groups,
            metadata_rows=metric_audit_state.metadata_rows,
            catalog_rows=metric_audit_state.catalog_rows,
            scope_season_rows=metric_audit_state.scope_season_rows,
            scope_team_rows=metric_audit_state.scope_team_rows,
            full_span_groups=metric_audit_state.full_span_groups,
            issues=issues,
        )

    return DatabaseValidationReport(issues=issues)


def _assert_valid_player_metrics_db() -> None:
    report = audit_player_metrics_db()
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
        signature = _normalize_issue_message(issue.message)
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


def _normalize_issue_message(message: str) -> str:
    normalized = _QUOTED_VALUE_PATTERN.sub("'<value>'", message)
    normalized = _NUMBER_PATTERN.sub("<num>", normalized)
    return " ".join(normalized.split())


def _validate_metric_store_relations(
    *,
    rawr_row_groups: dict[tuple[str, str], list[RawrPlayerSeasonValueRow]],
    wowy_row_groups: dict[tuple[str, str], list[WowyPlayerSeasonValueRow]],
    metadata_rows: dict[tuple[str, str], MetricStoreAuditMetadata],
    catalog_rows: dict[tuple[str, str], MetricScopeCatalogRow],
    scope_season_rows: dict[tuple[str, str], list[str]],
    scope_team_rows: dict[tuple[str, str], list[int]],
    full_span_groups: dict[
        tuple[str, str], tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]
    ],
    issues: list[ValidationIssue],
) -> None:
    metric_row_groups = rawr_row_groups | wowy_row_groups
    metric_scopes = set(metric_row_groups)
    metadata_scopes = set(metadata_rows)
    catalog_scopes = set(catalog_rows)
    scope_season_scopes = set(scope_season_rows)
    scope_team_scopes = set(scope_team_rows)
    full_span_scopes = set(full_span_groups)
    cache_load_counts, fingerprint_by_season_type = _load_normalized_cache_state()

    for key, rows in metric_row_groups.items():
        metric, scope_key = key
        seasons = sorted({row.season_id for row in rows})
        season_set = set(seasons)
        metadata_row = metadata_rows.get(key)
        metadata_table = (
            metadata_row.source_table if metadata_row is not None else "metric_snapshot"
        )
        if metadata_row is None:
            issues.append(
                ValidationIssue(
                    table=metadata_table,
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message="missing snapshot row for metric scope",
                )
            )
        else:
            row_snapshot_id = _metric_group_snapshot_id(rows)
            if row_snapshot_id != metadata_row.snapshot_id:
                issues.append(
                    ValidationIssue(
                        table=metadata_table,
                        key=f"metric={metric!r},scope_key={scope_key!r}",
                        message=(
                            "metric value rows point at a different snapshot_id than "
                            "metric_snapshot"
                        ),
                    )
                )
            if metadata_row.row_count != len(rows):
                issues.append(
                    ValidationIssue(
                        table=metadata_table,
                        key=f"metric={metric!r},scope_key={scope_key!r}",
                        message=(
                            "row_count does not match metric value rows:"
                            f"{metadata_row.row_count} != {len(rows)}"
                        ),
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
        elif not season_set.issubset(set(catalog_row.available_season_ids)):
            issues.append(
                ValidationIssue(
                    table="metric_scope_catalog",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message=(
                        "available_seasons is missing seasons present in metric value rows: "
                        f"catalog={catalog_row.available_season_ids!r} "
                        f"metric_value_rows={seasons!r}"
                    ),
                )
            )
        if (
            catalog_row is not None
            and scope_season_rows.get(key, []) != catalog_row.available_season_ids
        ):
            issues.append(
                ValidationIssue(
                    table="metric_scope_season",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message="scope-season rows do not match catalog available_season_ids",
                )
            )
        if (
            catalog_row is not None
            and scope_team_rows.get(key, []) != catalog_row.available_team_ids
        ):
            issues.append(
                ValidationIssue(
                    table="metric_scope_team",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message="scope-team rows do not match catalog available_team_ids",
                )
            )

    all_scopes = (
        metric_scopes
        | metadata_scopes
        | catalog_scopes
        | scope_season_scopes
        | scope_team_scopes
        | full_span_scopes
    )
    for key in sorted(all_scopes):
        metric, scope_key = key
        metadata_row = metadata_rows.get(key)
        metadata_table = (
            metadata_row.source_table if metadata_row is not None else "metric_snapshot"
        )
        catalog_row = catalog_rows.get(key)
        group_rows = metric_row_groups.get(key, [])
        season_type = (
            catalog_row.season_type
            if catalog_row is not None
            else _metric_group_season_type(group_rows)
            if group_rows
            else None
        )
        if season_type is None:
            continue
        if cache_load_counts.get(season_type, 0) == 0:
            issues.append(
                ValidationIssue(
                    table=metadata_table,
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message=(
                        "derived snapshot scope exists but normalized cache is empty for "
                        f"season_type {season_type!r}"
                    ),
                )
            )
            continue
        if metadata_row is None:
            continue
        current_fingerprint = fingerprint_by_season_type.get(season_type)
        if current_fingerprint is None:
            continue
        if metadata_row.source_fingerprint != current_fingerprint:
            issues.append(
                ValidationIssue(
                    table=metadata_row.source_table,
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message=(
                        "source_fingerprint does not match normalized cache for "
                        f"season_type {season_type!r}"
                    ),
                )
            )

    for key in sorted(metadata_scopes - metric_scopes):
        metadata_table = metadata_rows[key].source_table
        issues.append(
            ValidationIssue(
                table=metadata_table,
                key=f"metric={key[0]!r},scope_key={key[1]!r}",
                message="snapshot row has no matching metric value rows",
            )
        )

    for key in sorted(catalog_scopes - metric_scopes):
        issues.append(
            ValidationIssue(
                table="metric_scope_catalog",
                key=f"metric={key[0]!r},scope_key={key[1]!r}",
                message="catalog row has no matching metric value rows",
            )
        )

    for key in sorted(scope_team_scopes - catalog_scopes):
        issues.append(
            ValidationIssue(
                table="metric_scope_team",
                key=f"metric={key[0]!r},scope_key={key[1]!r}",
                message="scope-team rows have no matching catalog row",
            )
        )

    for key in sorted(scope_season_scopes - catalog_scopes):
        issues.append(
            ValidationIssue(
                table="metric_scope_season",
                key=f"metric={key[0]!r},scope_key={key[1]!r}",
                message="scope-season rows have no matching catalog row",
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
        metadata_row = metadata_rows.get(key)
        if metadata_row is not None:
            full_span_snapshot_id = _full_span_group_snapshot_id(series_rows, point_rows)
            if full_span_snapshot_id != metadata_row.snapshot_id:
                issues.append(
                    ValidationIssue(
                        table="metric_full_span",
                        key=f"metric={metric!r},scope_key={scope_key!r}",
                        message=(
                            "full-span rows point at a different snapshot_id than metric_snapshot"
                        ),
                    )
                )
        allowed_seasons = set(catalog_row.available_season_ids)
        for point_row in point_rows:
            if point_row.season_id not in allowed_seasons:
                issues.append(
                    ValidationIssue(
                        table="metric_full_span_points",
                        key=(
                            f"metric={metric!r},scope_key={scope_key!r},"
                            f"player_id={point_row.player_id!r},season={point_row.season_id!r}"
                        ),
                        message="point season is not present in catalog available_seasons",
                    )
                )
        if key in metric_row_groups and not series_rows and metric_row_groups[key]:
            issues.append(
                ValidationIssue(
                    table="metric_full_span_series",
                    key=f"metric={metric!r},scope_key={scope_key!r}",
                    message="metric value rows exist but no full-span series rows were stored",
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


def _metric_group_snapshot_id(
    rows: list[RawrPlayerSeasonValueRow] | list[WowyPlayerSeasonValueRow],
) -> int | None:
    assert rows, "metric group rows must not be empty"
    return rows[0].snapshot_id


def _full_span_group_snapshot_id(
    series_rows: list[MetricFullSpanSeriesRow],
    point_rows: list[MetricFullSpanPointRow],
) -> int | None:
    if series_rows:
        return series_rows[0].snapshot_id
    if point_rows:
        return point_rows[0].snapshot_id
    return None


def _metric_group_season_type(
    rows: list[RawrPlayerSeasonValueRow] | list[WowyPlayerSeasonValueRow],
) -> str:
    assert rows, "metric group rows must not be empty"
    return rows[0].season_type


def _load_normalized_cache_state() -> tuple[dict[str, int], dict[str, str]]:
    counts: dict[str, int] = {}
    fingerprints: dict[str, str] = {}
    for season_type in SeasonType:
        snapshot = load_cache_snapshot(season_type)
        counts[season_type.value] = len(snapshot.entries)
        if snapshot.entries:
            fingerprints[season_type.value] = snapshot.fingerprint
    return counts, fingerprints
