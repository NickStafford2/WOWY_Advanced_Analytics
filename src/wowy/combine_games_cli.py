from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any

from wowy.normalized_io import (
    NORMALIZED_GAME_PLAYERS_HEADER,
    NORMALIZED_GAMES_HEADER,
)

DEFAULT_GAMES_INPUT_DIR = Path("data/normalized/nba/games")
DEFAULT_GAME_PLAYERS_INPUT_DIR = Path("data/normalized/nba/game_players")
DEFAULT_GAMES_OUTPUT_PATH = Path("data/combined/regression/games.csv")
DEFAULT_GAME_PLAYERS_OUTPUT_PATH = Path("data/combined/regression/game_players.csv")


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for combining normalized regression input files."""

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


def combine_csvs(
    input_dir: Path,
    output_path: Path,
    expected_header: list[str],
) -> None:
    """Combine all CSV files in a directory that share the same expected header."""

    csv_paths = sorted(input_dir.glob("*.csv"))
    if not csv_paths:
        raise ValueError(f"No CSV files found in {input_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(expected_header)

        for csv_path in csv_paths:
            write_csv_rows(csv_path, writer, expected_header)


def combine_csv_paths(
    input_paths: list[Path],
    output_path: Path,
    expected_header: list[str],
) -> None:
    """Combine selected CSV files that share the same expected header."""

    if not input_paths:
        raise ValueError("No input CSV files provided")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(expected_header)

        for csv_path in input_paths:
            write_csv_rows(csv_path, writer, expected_header)


def write_csv_rows(
    csv_path: Path,
    writer: Any,
    expected_header: list[str],
) -> None:
    with open(csv_path, "r", encoding="utf-8", newline="") as input_file:
        reader = csv.reader(input_file)
        header = next(reader, None)
        if header != expected_header:
            raise ValueError(f"Unexpected CSV header in {csv_path}: {header!r}")
        for row in reader:
            writer.writerow(row)


def combine_normalized_files(
    games_input_paths: list[Path],
    game_players_input_paths: list[Path],
    games_output_path: Path,
    game_players_output_path: Path,
) -> None:
    """Combine selected normalized CSVs for regression input."""

    combine_csv_paths(games_input_paths, games_output_path, NORMALIZED_GAMES_HEADER)
    combine_csv_paths(
        game_players_input_paths,
        game_players_output_path,
        NORMALIZED_GAME_PLAYERS_HEADER,
    )


def combine_normalized_data(
    games_input_dir: Path,
    game_players_input_dir: Path,
    games_output_path: Path,
    game_players_output_path: Path,
) -> None:
    """Combine normalized game and game-player CSVs for regression input."""

    combine_csvs(games_input_dir, games_output_path, NORMALIZED_GAMES_HEADER)
    combine_csvs(
        game_players_input_dir,
        game_players_output_path,
        NORMALIZED_GAME_PLAYERS_HEADER,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the combine-games CLI."""

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
