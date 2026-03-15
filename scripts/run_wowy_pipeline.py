from __future__ import annotations

import argparse
import csv
from pathlib import Path

from wowy.cli import run_wowy
from wowy.ingest_nba import DEFAULT_WOWY_GAMES_DIR, write_team_season_games_csv


WOWY_HEADER = ["game_id", "season", "team", "margin", "players"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch team-seasons, combine WOWY team-game CSVs, and run the WOWY report."
    )
    parser.add_argument(
        "team_seasons",
        nargs="+",
        help="Team-season specs in TEAM:SEASON format, for example BOS:2023-24",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type, for example 'Regular Season' or 'Playoffs'",
    )
    parser.add_argument(
        "--combined-csv",
        type=Path,
        default=Path("data/combined/wowy/games.csv"),
        help="Combined WOWY games CSV path",
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


def parse_team_season_spec(spec: str) -> tuple[str, str]:
    team, separator, season = spec.partition(":")
    if not separator or not team or not season:
        raise ValueError(
            f"Invalid team-season spec {spec!r}. Expected TEAM:SEASON, for example BOS:2023-24."
        )
    return team.upper(), season


def combine_wowy_csvs(input_paths: list[Path], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(WOWY_HEADER)

        for input_path in input_paths:
            with open(input_path, "r", encoding="utf-8", newline="") as input_file:
                reader = csv.reader(input_file)
                header = next(reader, None)
                if header != WOWY_HEADER:
                    raise ValueError(
                        f"Unexpected WOWY CSV header in {input_path}: {header!r}"
                    )
                for row in reader:
                    writer.writerow(row)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    csv_paths: list[Path] = []
    for spec in args.team_seasons:
        team, season = parse_team_season_spec(spec)
        csv_path = DEFAULT_WOWY_GAMES_DIR / f"{team}_{season}.csv"
        write_team_season_games_csv(
            team_abbreviation=team,
            season=season,
            csv_path=csv_path,
            season_type=args.season_type,
        )
        csv_paths.append(csv_path)

    combine_wowy_csvs(csv_paths, args.combined_csv)
    print(
        run_wowy(
            args.combined_csv,
            min_games_with=args.min_games_with,
            min_games_without=args.min_games_without,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
