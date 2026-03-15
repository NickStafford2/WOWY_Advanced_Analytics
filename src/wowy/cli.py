from __future__ import annotations

import argparse
from pathlib import Path

from wowy.analysis import compute_wowy, filter_results
from wowy.formatting import format_results_table
from wowy.ingest_nba import DEFAULT_SOURCE_DATA_DIR, load_player_names_from_cache
from wowy.io import load_games_from_csv
from wowy.types import WowyGameRecord


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
    parser.add_argument(
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help="Path to cached source data used for player names",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=None,
        help="Maximum number of players to include in output",
    )
    return parser


def validate_filters(
    min_games_with: int,
    min_games_without: int,
    top_n: int | None = None,
) -> None:
    if min_games_with < 0 or min_games_without < 0:
        raise ValueError("Minimum game filters must be non-negative")
    if top_n is not None and top_n < 0:
        raise ValueError("Top-n filter must be non-negative")


def build_wowy_report(
    games: list[WowyGameRecord],
    min_games_with: int,
    min_games_without: int,
    player_names: dict[int, str] | None = None,
    top_n: int | None = None,
) -> str:
    results = compute_wowy(games)
    filtered_results = filter_results(
        results,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
    )
    return format_results_table(
        filtered_results,
        player_names=player_names,
        top_n=top_n,
    )


def run_wowy(
    csv_path: Path | str,
    min_games_with: int,
    min_games_without: int,
    player_names: dict[int, str] | None = None,
    top_n: int | None = None,
) -> str:
    validate_filters(min_games_with, min_games_without, top_n=top_n)
    games = load_games_from_csv(csv_path)
    return build_wowy_report(
        games,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        player_names=player_names,
        top_n=top_n,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    validate_filters(
        args.min_games_with,
        args.min_games_without,
        top_n=args.top_n,
    )
    player_names = load_player_names_from_cache(args.source_data_dir)
    print(
        run_wowy(
            args.csv,
            min_games_with=args.min_games_with,
            min_games_without=args.min_games_without,
            player_names=player_names,
            top_n=args.top_n,
        )
    )
    return 0
