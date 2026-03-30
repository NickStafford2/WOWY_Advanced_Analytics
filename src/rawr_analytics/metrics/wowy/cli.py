from __future__ import annotations

import argparse

from rawr_analytics.data.wowy import prepare_wowy_player_season_records
from rawr_analytics.metrics.wowy import validate_filters
from rawr_analytics.metrics.wowy._formatting import format_results_table
from rawr_analytics.progress import print_status_box
from rawr_analytics.shared.scope import format_scope
from rawr_analytics.shared.season import SeasonType


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run WOWY on normalized cached data.")
    parser.add_argument("--team", action="append", default=None)
    parser.add_argument("--season", action="append", default=None)
    parser.add_argument("--season-type", default="Regular Season")
    parser.add_argument("--min-games-with", type=int, default=15)
    parser.add_argument("--min-games-without", type=int, default=2)
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument("--min-average-minutes", type=float, default=30)
    parser.add_argument("--min-total-minutes", type=float, default=600)
    return parser


def _prepare_and_run_wowy(args) -> str:
    season_type = (
        args.season_type
        if isinstance(args.season_type, SeasonType)
        else SeasonType.parse(args.season_type)
    )
    validate_filters(
        args.min_games_with,
        args.min_games_without,
        top_n=args.top_n,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    print_status_box(
        "WOWY CLI",
        [
            f"Scope: {format_scope(args.team, args.season)}",
            "Preparing cached WOWY inputs and computing with/without player impact.",
            "Output is filtered after the metric pass using the requested sample and minute cuts.",
        ],
    )
    print(f"[1/2] preparing WOWY inputs for {format_scope(args.team, args.season)}")
    records = prepare_wowy_player_season_records(
        teams=args.team,
        seasons=args.season,
        season_type=season_type,
        min_games_with=args.min_games_with,
        min_games_without=args.min_games_without,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    print(f"[2/2] built {len(records)} player-season WOWY rows")
    return format_results_table(records, top_n=args.top_n)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    print(_prepare_and_run_wowy(args))
    return 0
