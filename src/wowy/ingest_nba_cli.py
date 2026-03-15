from __future__ import annotations

import argparse
from pathlib import Path

from wowy.ingest_nba import write_team_season_games_csv


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for NBA game ingestion."""

    parser = argparse.ArgumentParser(
        description="Fetch one NBA team-season and write games.csv in WOWY format."
    )
    parser.add_argument(
        "team",
        nargs="?",
        default="BOS",
        help="NBA team abbreviation, for example BOS",
    )
    parser.add_argument(
        "season",
        nargs="?",
        default="2023-24",
        help="NBA season string, for example 2023-24",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Output CSV path",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type, for example 'Regular Season' or 'Playoffs'",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the NBA ingestion CLI."""

    parser = build_parser()
    args = parser.parse_args(argv)
    csv_path = args.csv or Path("data/raw/nba/team_games") / f"{args.team.upper()}_{args.season}.csv"

    write_team_season_games_csv(
        team_abbreviation=args.team,
        season=args.season,
        csv_path=csv_path,
        season_type=args.season_type,
    )
    return 0
