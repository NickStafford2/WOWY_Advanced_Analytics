from __future__ import annotations

import argparse
from typing import Any

from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def add_metric_query_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--team", action="append", default=None)
    parser.add_argument("--season", action="append", default=None)
    parser.add_argument("--season-type", default="Regular Season")
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument("--min-average-minutes", type=float, default=30)
    parser.add_argument("--min-total-minutes", type=float, default=600)


def parse_metric_query_teams(raw_values: list[str] | None) -> list[Team] | None:
    if not raw_values:
        return None
    return [Team.from_abbreviation(raw_value) for raw_value in raw_values]


def parse_metric_query_season_type(raw_value: SeasonType | str) -> SeasonType:
    if isinstance(raw_value, SeasonType):
        return raw_value
    return SeasonType.parse(raw_value)


def parse_metric_query_seasons(
    raw_values: list[str] | None,
    *,
    season_type: SeasonType,
) -> list[Season] | None:
    if not raw_values:
        return None
    return [Season(raw_value, season_type.value) for raw_value in raw_values]


def render_metric_query_table(
    metric_label: str,
    rows: list[dict[str, Any]],
) -> str:
    column_order = _build_column_order(rows)
    display_rows = [
        [
            _format_cell(row.get(column), metric_label=metric_label, column=column)
            for column in column_order
        ]
        for row in rows
    ]
    headers = [_header_label(column, metric_label=metric_label) for column in column_order]
    widths = [
        max(len(header), *(len(display_row[index]) for display_row in display_rows))
        for index, header in enumerate(headers)
    ]
    lines = [
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)),
        "  ".join("-" * width for width in widths),
    ]
    lines.extend(
        "  ".join(
            (
                display_row[index].ljust(widths[index])
                if index == 1
                else display_row[index].rjust(widths[index])
            )
            for index in range(len(display_row))
        )
        for display_row in display_rows
    )
    return "\n".join(lines)


def _build_column_order(rows: list[dict[str, Any]]) -> list[str]:
    preferred_order = [
        "rank",
        "player_name",
        "span_average_value",
        "average_minutes",
        "total_minutes",
        "games_with",
        "games_without",
        "avg_margin_with",
        "avg_margin_without",
        "season_count",
    ]
    available_columns = {key for row in rows for key in row}
    return [column for column in preferred_order if column in available_columns]


def _header_label(column: str, *, metric_label: str) -> str:
    return {
        "rank": "Rank",
        "player_name": "Player",
        "span_average_value": metric_label,
        "average_minutes": "Avg Min",
        "total_minutes": "Tot Min",
        "games_with": "With",
        "games_without": "Without",
        "avg_margin_with": "Avg With",
        "avg_margin_without": "Avg Without",
        "season_count": "Seasons",
    }[column]


def _format_cell(value: Any, *, metric_label: str, column: str) -> str:
    del metric_label
    if value is None:
        return "—"
    if column == "player_name":
        return str(value)
    if isinstance(value, float):
        if column in {"span_average_value", "avg_margin_with", "avg_margin_without"}:
            return f"{value:.3f}"
        return f"{value:.1f}"
    return str(value)


__all__ = [
    "add_metric_query_common_arguments",
    "parse_metric_query_season_type",
    "parse_metric_query_seasons",
    "parse_metric_query_teams",
    "render_metric_query_table",
]
