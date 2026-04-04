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
from rawr_analytics.cli._progress_bar import TerminalProgressBar, print_status_box
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr import build_rawr_query
from rawr_analytics.services.rawr_query import build_rawr_query_export
from rawr_analytics.shared.season import Season


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
    total_seasons = len(query.seasons or [])
    load_bar = TerminalProgressBar("Season load", total=max(1, total_seasons))
    print("[1/3] loading season inputs", flush=True)
    progress_fn = _build_progress_updater(load_bar)
    rows = build_rawr_query_export(
        query,
        view="custom-query",
        progress_fn=progress_fn,
    )
    load_bar.finish(detail="season inputs ready")
    print("[2/3] computed rawr rankings", flush=True)
    print(f"[3/3] rendering {len(rows)} leaderboard rows", flush=True)
    print(render_metric_query_table(Metric.RAWR, rows))
    return 0


def _update_rawr_progress(
    progress_bar: TerminalProgressBar,
    *,
    current: int,
    total: int,
    season: Season,
) -> None:
    if progress_bar.total != max(1, total):
        progress_bar.total = max(1, total)
    progress_bar.update(current, detail=season.id)


def _build_progress_updater(
    progress_bar: TerminalProgressBar,
):
    def update(current: int, total: int, season: Season) -> None:
        _update_rawr_progress(
            progress_bar,
            current=current,
            total=total,
            season=season,
        )

    return update


def _run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except ValueError as exc:
        sys.stderr.write(f"{exc}\n")
        sys.stderr.flush()
        return 1
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(_run())
