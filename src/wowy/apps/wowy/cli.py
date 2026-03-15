from __future__ import annotations

import argparse
from pathlib import Path

from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    load_player_names_from_cache,
)
from wowy.apps.wowy.service import prepare_and_run_wowy


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run WOWY on cached data, fetching missing requested scope when needed."
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
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help="Path to cached source data used for player names",
    )
    parser.add_argument(
        "--normalized-games-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAMES_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--normalized-game-players-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--wowy-output-dir",
        type=Path,
        default=DEFAULT_WOWY_GAMES_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--combined-wowy-csv",
        type=Path,
        default=Path("data/combined/wowy/games.csv"),
        help=argparse.SUPPRESS,
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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    print(
        prepare_and_run_wowy(
            args,
            load_player_names_fn=load_player_names_from_cache,
        )
    )
    return 0
