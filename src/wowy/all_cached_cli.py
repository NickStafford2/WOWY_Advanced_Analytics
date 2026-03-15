from __future__ import annotations

import argparse
from pathlib import Path

from wowy.cli import run_wowy
from wowy.combine_games_cli import combine_csv_paths, combine_normalized_data
from wowy.ingest_nba import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    load_player_names_from_cache,
)
from wowy.regression_cli import run_regression


WOWY_HEADER = ["game_id", "team", "margin", "players"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Combine all cached data and run both WOWY and regression reports."
    )
    parser.add_argument(
        "--wowy-input-dir",
        type=Path,
        default=DEFAULT_WOWY_GAMES_DIR,
        help="Directory containing cached WOWY team-game CSV files",
    )
    parser.add_argument(
        "--normalized-games-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAMES_DIR,
        help="Directory containing cached normalized games CSV files",
    )
    parser.add_argument(
        "--normalized-game-players-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
        help="Directory containing cached normalized game-player CSV files",
    )
    parser.add_argument(
        "--combined-wowy-csv",
        type=Path,
        default=Path("data/combined/wowy/games.csv"),
        help="Combined WOWY games CSV path",
    )
    parser.add_argument(
        "--combined-regression-games-csv",
        type=Path,
        default=Path("data/combined/regression/games.csv"),
        help="Combined normalized games CSV path",
    )
    parser.add_argument(
        "--combined-regression-game-players-csv",
        type=Path,
        default=Path("data/combined/regression/game_players.csv"),
        help="Combined normalized game-player CSV path",
    )
    parser.add_argument(
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help="Path to cached source data used for player names",
    )
    parser.add_argument(
        "--min-games-with",
        type=int,
        default=2,
        help="Minimum games with player required to include player in WOWY output",
    )
    parser.add_argument(
        "--min-games-without",
        type=int,
        default=2,
        help="Minimum games without player required to include player in WOWY output",
    )
    parser.add_argument(
        "--wowy-top-n",
        type=int,
        default=None,
        help="Maximum number of players to include in WOWY output",
    )
    parser.add_argument(
        "--min-regression-games",
        type=int,
        default=1,
        help="Minimum games required to include a player in regression output",
    )
    parser.add_argument(
        "--ridge-alpha",
        type=float,
        default=1.0,
        help="Ridge regularization strength for player coefficients",
    )
    parser.add_argument(
        "--regression-top-n",
        type=int,
        default=None,
        help="Maximum number of players to include in regression output",
    )
    return parser


def run_all_cached(
    wowy_input_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    combined_wowy_csv: Path,
    combined_regression_games_csv: Path,
    combined_regression_game_players_csv: Path,
    source_data_dir: Path,
    min_games_with: int,
    min_games_without: int,
    wowy_top_n: int | None,
    min_regression_games: int,
    ridge_alpha: float,
    regression_top_n: int | None,
) -> str:
    wowy_paths = sorted(wowy_input_dir.glob("*.csv"))
    print(f"[1/5] combining WOWY inputs from {len(wowy_paths)} cached files")
    combine_csv_paths(wowy_paths, combined_wowy_csv, WOWY_HEADER)

    normalized_game_paths = sorted(normalized_games_input_dir.glob("*.csv"))
    normalized_player_paths = sorted(normalized_game_players_input_dir.glob("*.csv"))
    print(
        "[2/5] combining regression inputs from "
        f"{len(normalized_game_paths)} game files and "
        f"{len(normalized_player_paths)} player files"
    )
    combine_normalized_data(
        games_input_dir=normalized_games_input_dir,
        game_players_input_dir=normalized_game_players_input_dir,
        games_output_path=combined_regression_games_csv,
        game_players_output_path=combined_regression_game_players_csv,
    )

    print(f"[3/5] loading player names from {source_data_dir}")
    player_names = load_player_names_from_cache(source_data_dir)
    print(f"[4/5] running WOWY on {combined_wowy_csv}")

    wowy_report = run_wowy(
        combined_wowy_csv,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        player_names=player_names,
        top_n=wowy_top_n,
    )
    print(f"[5/5] running regression on {combined_regression_games_csv}")
    regression_report = run_regression(
        combined_regression_games_csv,
        combined_regression_game_players_csv,
        min_games=min_regression_games,
        ridge_alpha=ridge_alpha,
        top_n=regression_top_n,
    )
    return f"{wowy_report}\n\n{regression_report}"


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    print(
        run_all_cached(
            wowy_input_dir=args.wowy_input_dir,
            normalized_games_input_dir=args.normalized_games_input_dir,
            normalized_game_players_input_dir=args.normalized_game_players_input_dir,
            combined_wowy_csv=args.combined_wowy_csv,
            combined_regression_games_csv=args.combined_regression_games_csv,
            combined_regression_game_players_csv=args.combined_regression_game_players_csv,
            source_data_dir=args.source_data_dir,
            min_games_with=args.min_games_with,
            min_games_without=args.min_games_without,
            wowy_top_n=args.wowy_top_n,
            min_regression_games=args.min_regression_games,
            ridge_alpha=args.ridge_alpha,
            regression_top_n=args.regression_top_n,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
