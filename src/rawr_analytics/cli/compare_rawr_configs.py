from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rawr_analytics.nba.source.cache import DEFAULT_SOURCE_DATA_DIR
from rawr_analytics.progress import TerminalProgressBar, print_status_box
from rawr_analytics.services.compare_rawr_configs import (
    CompareRawrConfigsProgress,
    CompareRawrConfigsRequest,
    build_compare_rawr_configs_summary,
    build_compare_rawr_configs_table,
    compare_rawr_configs,
)
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare WOWY and RAWR training configurations against a holdout-season WOWY target."
        )
    )
    parser.add_argument(
        "--train-season",
        action="append",
        required=True,
        help="Training season. Repeat to include multiple seasons.",
    )
    parser.add_argument(
        "--holdout-season",
        required=True,
        help="Unseen season used only for evaluation.",
    )
    parser.add_argument(
        "--team",
        action="append",
        default=None,
        help="Optional team abbreviation filter. Repeat to include multiple teams.",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="Season type used when preparing inputs.",
    )
    parser.add_argument(
        "--aggregation",
        choices=["mean", "max", "latest"],
        default="mean",
        help="How to collapse multiple training seasons into one player score.",
    )
    parser.add_argument(
        "--rawr-ridge-grid",
        default="1,3,10,30",
        help="Comma-separated ridge alpha values to evaluate.",
    )
    parser.add_argument(
        "--shrinkage-mode",
        action="append",
        choices=["uniform", "game-count", "minutes"],
        default=None,
        help="RAWR shrinkage mode to evaluate. Repeat to include multiple modes.",
    )
    parser.add_argument(
        "--shrinkage-strength-grid",
        default="0,0.5,1.0",
        help="Comma-separated shrinkage strengths to evaluate.",
    )
    parser.add_argument(
        "--shrinkage-minute-scale-grid",
        default="48,240",
        help="Comma-separated minute scales to evaluate for minute-aware shrinkage.",
    )
    parser.add_argument(
        "--rawr-min-games",
        type=int,
        default=35,
        help="Minimum games required for a RAWR player-season record.",
    )
    parser.add_argument(
        "--holdout-min-games-with",
        type=int,
        default=15,
        help="Minimum games with player for holdout WOWY target rows.",
    )
    parser.add_argument(
        "--holdout-min-games-without",
        type=int,
        default=2,
        help="Minimum games without player for holdout WOWY target rows.",
    )
    parser.add_argument(
        "--min-average-minutes",
        type=float,
        default=30.0,
        help="Minimum average minutes filter applied to both training and holdout rows.",
    )
    parser.add_argument(
        "--min-total-minutes",
        type=float,
        default=600.0,
        help="Minimum total minutes filter applied to both training and holdout rows.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Top-N cutoff used for overlap reporting.",
    )
    parser.add_argument(
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    season_type = SeasonType.parse(args.season_type)
    request = CompareRawrConfigsRequest(
        train_seasons=[Season(season, season_type.value) for season in args.train_season],
        holdout_season=Season(args.holdout_season, season_type.value),
        season_type=season_type,
        aggregation=args.aggregation,
        teams=_parse_teams(args.team, season=Season(args.holdout_season, season_type.value)),
        rawr_ridge_values=_parse_float_grid(args.rawr_ridge_grid),
        shrinkage_modes=args.shrinkage_mode,
        shrinkage_strength_values=_parse_float_grid(args.shrinkage_strength_grid),
        shrinkage_minute_scale_values=_parse_float_grid(args.shrinkage_minute_scale_grid),
        rawr_min_games=args.rawr_min_games,
        holdout_min_games_with=args.holdout_min_games_with,
        holdout_min_games_without=args.holdout_min_games_without,
        min_average_minutes=args.min_average_minutes,
        min_total_minutes=args.min_total_minutes,
        top_n=args.top_n,
    )

    print_status_box(
        "RAWR Tuning",
        [
            (
                f"Training on {', '.join(args.train_season)} and evaluating against "
                f"holdout {args.holdout_season}."
            ),
            (
                "The progress bar below tracks holdout preparation, baseline WOWY, "
                "and each RAWR configuration in the requested sweep."
            ),
        ],
    )
    progress_bar = TerminalProgressBar("RAWR tune", total=1)
    results = compare_rawr_configs(
        request,
        event_fn=lambda event: _update_progress(progress_bar, event),
    )
    progress_bar.finish(detail="done")
    print(build_compare_rawr_configs_summary(request, results))
    print(build_compare_rawr_configs_table(results))
    return 0


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


def _parse_float_grid(raw_value: str) -> list[float]:
    values = [part.strip() for part in raw_value.split(",")]
    if not values or any(not value for value in values):
        raise ValueError("Grid values must contain one or more comma-separated numbers")
    parsed = [float(value) for value in values]
    if any(value < 0.0 for value in parsed):
        raise ValueError("Grid values must be non-negative")
    return parsed


def _update_progress(
    progress_bar: TerminalProgressBar,
    event: CompareRawrConfigsProgress,
) -> None:
    progress_bar.total = max(event.total, 1)
    progress_bar.update(event.current, detail=event.detail)


def _parse_teams(raw_values: list[str] | None, *, season: Season) -> list[Team] | None:
    if raw_values is None:
        return None
    return [Team.from_abbreviation(raw_value, season=season) for raw_value in raw_values]


if __name__ == "__main__":
    raise SystemExit(run())
