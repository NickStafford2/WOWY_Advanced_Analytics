from __future__ import annotations

import argparse
import csv
from pathlib import Path


EXPECTED_HEADER = ["game_id", "team", "margin", "players"]


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for combining normalized game CSV files."""

    parser = argparse.ArgumentParser(
        description="Combine normalized WOWY game CSV files into one output file."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/raw/nba/team_games"),
        help="Directory containing normalized game CSV files",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/combined/wowy/games.example.csv"),
        help="Combined output CSV path",
    )
    return parser


def combine_game_csvs(input_dir: Path, output_path: Path) -> None:
    """Combine all normalized game CSV files in a directory into one CSV."""

    csv_paths = sorted(input_dir.glob("*.csv"))
    if not csv_paths:
        raise ValueError(f"No CSV files found in {input_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(EXPECTED_HEADER)

        for csv_path in csv_paths:
            with open(csv_path, "r", encoding="utf-8", newline="") as input_file:
                reader = csv.reader(input_file)
                header = next(reader, None)
                if header != EXPECTED_HEADER:
                    raise ValueError(f"Unexpected CSV header in {csv_path}: {header!r}")
                for row in reader:
                    writer.writerow(row)


def main(argv: list[str] | None = None) -> int:
    """Run the combine-games CLI."""

    parser = build_parser()
    args = parser.parse_args(argv)
    combine_game_csvs(args.input_dir, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
