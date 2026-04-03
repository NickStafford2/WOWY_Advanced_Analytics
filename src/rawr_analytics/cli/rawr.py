from __future__ import annotations

import argparse
import sys

from rawr_analytics.cli._metric_query_cli import (
    add_metric_query_common_arguments,
    parse_metric_query_season_type,
    parse_metric_query_seasons,
    parse_metric_query_teams,
    render_metric_query_table,
)
from rawr_analytics.cli._progress_bar import print_status_box
from rawr_analytics.metrics.rawr import build_rawr_query
from rawr_analytics.services import build_rawr_query_export
from rawr_analytics.shared.scope import format_scope


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the RAWR custom query used by the web app.")
    add_metric_query_common_arguments(parser)
    parser.add_argument("--min-games", type=int, default=35)
    parser.add_argument("--ridge-alpha", type=float, default=10.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    season_type = parse_metric_query_season_type(args.season_type)
    query = build_rawr_query(
        season_type=season_type,
        teams=parse_metric_query_teams(args.team),
        seasons=parse_metric_query_seasons(args.season, season_type=season_type),
        top_n=args.top_n,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
        min_games=args.min_games,
        ridge_alpha=args.ridge_alpha,
    )
    print_status_box(
        "RAWR CLI",
        [
            f"Scope: {format_scope(args.team, args.season)}",
            "Running the same custom-query service path used by the web app.",
            "Output is a terminal leaderboard built from the shared service export rows.",
        ],
    )
    print("[1/2] building rawr custom query")
    result = build_rawr_query_export(query, view="custom-query")
    print(f"[2/2] built {len(result.rows)} leaderboard rows")
    print(render_metric_query_table(result.metric_label, result.rows))
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
