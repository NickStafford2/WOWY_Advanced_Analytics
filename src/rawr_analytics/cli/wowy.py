from __future__ import annotations

import argparse
import sys

from rawr_analytics.cli._common import format_scope
from rawr_analytics.cli._metric_query_cli import (
    add_metric_query_common_arguments,
    parse_metric_query_season_type,
    parse_metric_query_seasons,
    parse_metric_query_teams,
    render_metric_query_table,
)
from rawr_analytics.cli._progress_bar import print_status_box
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.query.request import build_wowy_query
from rawr_analytics.metrics.wowy.query.service import (
    build_wowy_export_rows_from_values,
    resolve_wowy_result,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the WOWY custom query used by the web app.")
    add_metric_query_common_arguments(parser)
    parser.add_argument(
        "--min-games-with",
        type=int,
        default=15,
        help="Pre-record minimum games with player for WOWY eligibility.",
    )
    parser.add_argument(
        "--min-games-without",
        type=int,
        default=4,
        help="Pre-record minimum games without player for WOWY eligibility.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    season_type = parse_metric_query_season_type(args.season_type)
    query = build_wowy_query(
        teams=parse_metric_query_teams(args.team),
        seasons=parse_metric_query_seasons(args.season, season_type=season_type),
        top_n=args.top_n,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
        min_games_with=args.min_games_with,
        min_games_without=args.min_games_without,
    )
    print_status_box(
        "WOWY CLI",
        [
            f"Scope: {format_scope(args.team, args.season)}",
            "Running the same custom-query service path used by the web app.",
            "Output is a terminal leaderboard built from the shared service export rows.",
        ],
    )
    print(f"[1/2] building {Metric.WOWY.value} custom query")
    result = resolve_wowy_result(query, metric=Metric.WOWY, recalculate=True)

    rows = build_wowy_export_rows_from_values(
        rows=result.player_season_value, seasons=result.seasons
    )
    print(f"[2/2] built {len(rows)} leaderboard rows")
    print(render_metric_query_table(Metric.WOWY, rows))
    return 0


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
