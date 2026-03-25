from __future__ import annotations

import argparse
from pathlib import Path

from wowy.metrics.rawr.service import prepare_and_run_rawr
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.source.cache import DEFAULT_SOURCE_DATA_DIR


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run RAWR on cached data, fetching missing requested scope when needed."
    )
    parser.add_argument(
        "--team",
        action="append",
        default=None,
        help="Filter to a team abbreviation. Repeat to include multiple teams.",
    )
    parser.add_argument(
        "--season",
        action="append",
        default=None,
        help="Filter to a season string. Repeat to include multiple seasons.",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to fetch when requested scope is missing from cache",
    )
    parser.add_argument(
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=35,
        help="Minimum games required to include a player in the RAWR output (default: 35)",
    )
    parser.add_argument(
        "--ridge-alpha",
        type=float,
        default=10.0,
        help="Ridge regularization strength for player coefficients",
    )
    parser.add_argument(
        "--shrinkage-mode",
        choices=["uniform", "game-count", "minutes"],
        default="uniform",
        help="Player-coefficient shrinkage mode",
    )
    parser.add_argument(
        "--shrinkage-strength",
        type=float,
        default=1.0,
        help="Exponent for game-count-aware player shrinkage",
    )
    parser.add_argument(
        "--shrinkage-minute-scale",
        type=float,
        default=48.0,
        help="Minute exposure treated as one unit for minute-aware shrinkage",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=40,
        help="Maximum number of players to include in output",
    )
    parser.add_argument(
        "--tune-ridge",
        action="store_true",
        help="Choose ridge alpha from a validation split before fitting the final model",
    )
    parser.add_argument(
        "--ridge-grid",
        default="0.3,1,3,10,30,100",
        help="Comma-separated ridge alphas to evaluate when --tune-ridge is used",
    )
    parser.add_argument(
        "--validation-fraction",
        type=float,
        default=0.2,
        help="Fraction of games held out for ridge tuning when --tune-ridge is used",
    )
    parser.add_argument(
        "--min-average-minutes",
        type=float,
        default=30,
        help="Minimum average minutes per appeared game required to include a player in output",
    )
    parser.add_argument(
        "--min-total-minutes",
        type=float,
        default=600,
        help="Minimum total minutes required to include a player in output",
    )
    parser.add_argument(
        "--player-metrics-db-path",
        type=Path,
        default=DEFAULT_PLAYER_METRICS_DB_PATH,
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    print(prepare_and_run_rawr(args))
    return 0
