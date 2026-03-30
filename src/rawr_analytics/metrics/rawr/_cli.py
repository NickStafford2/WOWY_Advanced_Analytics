from __future__ import annotations

import argparse
from pathlib import Path

from rawr_analytics.data.rawr import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    prepare_rawr_player_season_records,
)
from rawr_analytics.metrics.rawr import validate_filters
from rawr_analytics.metrics.rawr._formatting import format_rawr_records
from rawr_analytics.nba.source.cache import DEFAULT_SOURCE_DATA_DIR
from rawr_analytics.progress import print_status_box
from rawr_analytics.shared.scope import format_scope
from rawr_analytics.shared.season import SeasonType


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run RAWR on cached normalized data.")
    parser.add_argument("--team", action="append", default=None)
    parser.add_argument("--season", action="append", default=None)
    parser.add_argument("--season-type", default="Regular Season")
    parser.add_argument(
        "--source-data-dir", type=Path, default=DEFAULT_SOURCE_DATA_DIR, help=argparse.SUPPRESS
    )
    parser.add_argument("--min-games", type=int, default=35)
    parser.add_argument("--ridge-alpha", type=float, default=10.0)
    parser.add_argument(
        "--shrinkage-mode", choices=["uniform", "game-count", "minutes"], default="uniform"
    )
    parser.add_argument("--shrinkage-strength", type=float, default=1.0)
    parser.add_argument("--shrinkage-minute-scale", type=float, default=48.0)
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument("--min-average-minutes", type=float, default=30)
    parser.add_argument("--min-total-minutes", type=float, default=600)
    return parser


def _prepare_and_run_rawr(args) -> str:
    season_type = (
        args.season_type
        if isinstance(args.season_type, SeasonType)
        else SeasonType.parse(args.season_type)
    )
    validate_filters(
        min_games=args.min_games,
        ridge_alpha=args.ridge_alpha,
        shrinkage_mode=args.shrinkage_mode,
        shrinkage_strength=args.shrinkage_strength,
        shrinkage_minute_scale=args.shrinkage_minute_scale,
        top_n=args.top_n,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    print_status_box(
        "RAWR CLI",
        [
            f"Scope: {format_scope(args.team, args.season)}",
            "Preparing normalized RAWR inputs and fitting the per-season ridge model.",
            "Output is filtered after model fit using the requested minute cuts.",
        ],
    )
    print(f"[1/2] preparing RAWR inputs for {format_scope(args.team, args.season)}")
    records = prepare_rawr_player_season_records(
        teams=args.team,
        seasons=args.season,
        season_type=season_type,
        min_games=args.min_games,
        ridge_alpha=args.ridge_alpha,
        shrinkage_mode=args.shrinkage_mode or DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=args.shrinkage_strength or DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=args.shrinkage_minute_scale or DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
    )
    print(f"[2/2] built {len(records)} player-season RAWR rows")
    return format_rawr_records(records, top_n=args.top_n)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    print(_prepare_and_run_rawr(args))
    return 0
