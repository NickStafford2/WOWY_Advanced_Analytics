from __future__ import annotations

import argparse
from pathlib import Path

from wowy.analysis import compute_wowy, filter_results
from wowy.formatting import print_results
from wowy.ingest_nba import load_player_names_from_cache
from wowy.io import load_games_from_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute a simple game-level WOWY score from a CSV file."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("data/combined/wowy/games.csv"),
        help="Path to the games CSV file",
    )
    parser.add_argument(
        "--min-games-with",
        type=int,
        default=2,
        help="Minimum games with player required to include player in output",
    )
    parser.add_argument(
        "--min-games-without",
        type=int,
        default=2,
        help="Minimum games without player required to include player in output",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.min_games_with < 0 or args.min_games_without < 0:
        raise ValueError("Minimum game filters must be non-negative")

    games = load_games_from_csv(args.csv)
    results = compute_wowy(games)
    filtered_results = filter_results(
        results,
        min_games_with=args.min_games_with,
        min_games_without=args.min_games_without,
    )
    player_names = load_player_names_from_cache()
    print_results(filtered_results, player_names=player_names)
    return 0
