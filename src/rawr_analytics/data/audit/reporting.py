from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass

from rawr_analytics.data._validation_issue import ValidationIssue


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
