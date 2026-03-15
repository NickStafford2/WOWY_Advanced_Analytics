from __future__ import annotations

import argparse
from pathlib import Path

from wowy.data.combine import combine_normalized_data


DEFAULT_GAMES_INPUT_DIR = Path("data/normalized/nba/games")
DEFAULT_GAME_PLAYERS_INPUT_DIR = Path("data/normalized/nba/game_players")
DEFAULT_GAMES_OUTPUT_PATH = Path("data/combined/regression/games.csv")
DEFAULT_GAME_PLAYERS_OUTPUT_PATH = Path("data/combined/regression/game_players.csv")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Combine normalized game and game-player CSV files for regression input."
    )
    parser.add_argument(
        "--games-input-dir",
        type=Path,
        default=DEFAULT_GAMES_INPUT_DIR,
        help="Directory containing normalized games CSV files",
    )
    parser.add_argument(
        "--game-players-input-dir",
        type=Path,
        default=DEFAULT_GAME_PLAYERS_INPUT_DIR,
        help="Directory containing normalized game-player CSV files",
    )
    parser.add_argument(
        "--games-output",
        type=Path,
        default=DEFAULT_GAMES_OUTPUT_PATH,
        help="Combined normalized games CSV path",
    )
    parser.add_argument(
        "--game-players-output",
        type=Path,
        default=DEFAULT_GAME_PLAYERS_OUTPUT_PATH,
        help="Combined normalized game-player CSV path",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    combine_normalized_data(
        games_input_dir=args.games_input_dir,
        game_players_input_dir=args.game_players_input_dir,
        games_output_path=args.games_output,
        game_players_output_path=args.game_players_output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
