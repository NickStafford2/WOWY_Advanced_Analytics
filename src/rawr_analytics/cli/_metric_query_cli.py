from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any

from rawr_analytics.cli._progress_bar import print_status_box
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.services import MetricQueryRequest, build_metric_query_export
from rawr_analytics.shared.scope import format_scope
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def build_metric_query_parser(
    *,
    description: str,
    include_rawr_options: bool,
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--team", action="append", default=None)
    parser.add_argument("--season", action="append", default=None)
    parser.add_argument("--season-type", default="Regular Season")
    if include_rawr_options:
        parser.add_argument("--min-games", type=int, default=35)
        parser.add_argument("--ridge-alpha", type=float, default=10.0)
    else:
        parser.add_argument("--min-games-with", type=int, default=15)
        parser.add_argument("--min-games-without", type=int, default=2)
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument("--min-average-minutes", type=float, default=30)
    parser.add_argument("--min-total-minutes", type=float, default=600)
    return parser


def run_metric_query_cli(
    args: argparse.Namespace,
    *,
    metric: Metric,
    title: str,
    details: Sequence[str],
) -> int:
    request = _build_metric_query_request(args, metric=metric)
    print_status_box(title, [f"Scope: {format_scope(args.team, args.season)}", *details])
    print(f"[1/2] building {metric.value} custom query")
    result = build_metric_query_export(request, view="custom-query")
    print(f"[2/2] built {len(result.rows)} leaderboard rows")
    print(_render_metric_query_table(result.metric_label, result.rows))
    return 0


def _build_metric_query_request(
    args: argparse.Namespace,
    *,
    metric: Metric,
) -> MetricQueryRequest:
    season_type = (
        args.season_type
        if isinstance(args.season_type, SeasonType)
        else SeasonType.parse(args.season_type)
    )
    seasons = _parse_seasons(args.season, season_type=season_type)
    return MetricQueryRequest(
        metric=metric,
        season_type=season_type,
        teams=_parse_teams(args.team),
        seasons=seasons,
        top_n=args.top_n,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
        min_games=(args.min_games if metric == Metric.RAWR else None),
        ridge_alpha=(args.ridge_alpha if metric == Metric.RAWR else None),
        min_games_with=(args.min_games_with if metric != Metric.RAWR else None),
        min_games_without=(args.min_games_without if metric != Metric.RAWR else None),
    )


def _parse_teams(raw_values: list[str] | None) -> list[Team] | None:
    if not raw_values:
        return None
    return [Team.from_abbreviation(raw_value) for raw_value in raw_values]


def _parse_seasons(
    raw_values: list[str] | None,
    *,
    season_type: SeasonType,
) -> list[Season] | None:
    if not raw_values:
        return None
    return [Season(raw_value, season_type.value) for raw_value in raw_values]


def _render_metric_query_table(
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
