from __future__ import annotations

import argparse
from pathlib import Path

from wowy.ingest_nba import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    write_team_season_games_csv,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for NBA game ingestion."""

    parser = argparse.ArgumentParser(
        description="Fetch one NBA team-season, write normalized CSVs, and derive WOWY games.csv."
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
        help="Output WOWY games CSV path",
    )
    parser.add_argument(
        "--normalized-games-csv",
        type=Path,
        default=None,
        help="Output normalized games CSV path",
    )
    parser.add_argument(
        "--normalized-game-players-csv",
        type=Path,
        default=None,
        help="Output normalized game-player CSV path",
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
    team = args.team.upper()
    csv_path = args.csv or DEFAULT_WOWY_GAMES_DIR / f"{team}_{args.season}.csv"
    normalized_games_csv = (
        args.normalized_games_csv or DEFAULT_NORMALIZED_GAMES_DIR / f"{team}_{args.season}.csv"
    )
    normalized_game_players_csv = (
        args.normalized_game_players_csv
        or DEFAULT_NORMALIZED_GAME_PLAYERS_DIR / f"{team}_{args.season}.csv"
    )

    write_team_season_games_csv(
        team_abbreviation=team,
        season=args.season,
        csv_path=csv_path,
        season_type=args.season_type,
        normalized_games_csv_path=normalized_games_csv,
        normalized_game_players_csv_path=normalized_game_players_csv,
    )
    return 0
