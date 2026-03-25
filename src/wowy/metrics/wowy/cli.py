from __future__ import annotations

import argparse
from pathlib import Path

from wowy.apps.wowy.service import prepare_and_run_wowy
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run WOWY on normalized cached data."
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
        help="NBA season type to read from normalized cache",
    )
    parser.add_argument(
        "--min-games-with",
        type=int,
        default=15,
        help="Minimum games with player required to include player in output (default: 15)",
    )
    parser.add_argument(
        "--min-games-without",
        type=int,
        default=2,
        help="Minimum games without player required to include player in output",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=40,
        help="Maximum number of players to include in output (default: 40)",
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
    print(prepare_and_run_wowy(args))
    return 0
