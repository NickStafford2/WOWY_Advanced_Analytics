from __future__ import annotations

import argparse
from pathlib import Path

from wowy.combine_games_cli import combine_normalized_data
from wowy.ingest_nba import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    write_team_season_games_csv,
)
from wowy.regression_cli import run_regression


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch team-seasons, combine normalized regression inputs, and run the regression report."
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
        "--games-output",
        type=Path,
        default=Path("data/combined/regression/games.csv"),
        help="Combined normalized games CSV path",
    )
    parser.add_argument(
        "--game-players-output",
        type=Path,
        default=Path("data/combined/regression/game_players.csv"),
        help="Combined normalized game-player CSV path",
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=1,
        help="Minimum games required to include a player in the regression output",
    )
    parser.add_argument(
        "--ridge-alpha",
        type=float,
        default=1.0,
        help="Ridge regularization strength for player coefficients",
    )
    return parser


def parse_team_season_spec(spec: str) -> tuple[str, str]:
    team, separator, season = spec.partition(":")
    if not separator or not team or not season:
        raise ValueError(
            f"Invalid team-season spec {spec!r}. Expected TEAM:SEASON, for example BOS:2023-24."
        )
    return team.upper(), season


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    for spec in args.team_seasons:
        team, season = parse_team_season_spec(spec)
        write_team_season_games_csv(
            team_abbreviation=team,
            season=season,
            csv_path=DEFAULT_WOWY_GAMES_DIR / f"{team}_{season}.csv",
            normalized_games_csv_path=DEFAULT_NORMALIZED_GAMES_DIR / f"{team}_{season}.csv",
            normalized_game_players_csv_path=DEFAULT_NORMALIZED_GAME_PLAYERS_DIR
            / f"{team}_{season}.csv",
            season_type=args.season_type,
        )

    combine_normalized_data(
        games_input_dir=DEFAULT_NORMALIZED_GAMES_DIR,
        game_players_input_dir=DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
        games_output_path=args.games_output,
        game_players_output_path=args.game_players_output,
    )
    print(
        run_regression(
            args.games_output,
            args.game_players_output,
            min_games=args.min_games,
            ridge_alpha=args.ridge_alpha,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
