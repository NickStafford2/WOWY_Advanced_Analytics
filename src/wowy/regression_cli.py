from __future__ import annotations

import argparse
from pathlib import Path

from wowy.cache_pipeline import prepare_regression_inputs
from wowy.ingest_nba import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)
from wowy.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)
from wowy.regression_analysis import fit_player_regression
from wowy.regression_data import build_regression_observations
from wowy.regression_formatting import format_regression_results


def format_scope(teams: list[str] | None, seasons: list[str] | None) -> str:
    team_label = ",".join(team.upper() for team in teams) if teams else "all cached teams"
    season_label = ",".join(seasons) if seasons else "all cached seasons"
    return f"teams={team_label} seasons={season_label}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run regression on cached data, fetching missing requested scope when needed."
    )
    parser.add_argument(
        "--games-csv",
        type=Path,
        default=None,
        help="Optional explicit combined normalized games CSV path",
    )
    parser.add_argument(
        "--game-players-csv",
        type=Path,
        default=None,
        help="Optional explicit combined normalized game-player CSV path",
    )
    parser.add_argument(
        "--team",
        action="append",
        default=None,
        help="Filter to a team abbreviation. Repeat to include multiple teams.",
    )
    parser.add_argument(
        "--season",
        action="append",
        default=None,
        help="Filter to a season string. Repeat to include multiple seasons.",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to fetch when requested scope is missing from cache",
    )
    parser.add_argument(
        "--source-data-dir",
        type=Path,
        default=DEFAULT_SOURCE_DATA_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--normalized-games-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAMES_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--normalized-game-players-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--wowy-output-dir",
        type=Path,
        default=DEFAULT_WOWY_GAMES_DIR,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--combined-games-csv",
        type=Path,
        default=Path("data/combined/regression/games.csv"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--combined-game-players-csv",
        type=Path,
        default=Path("data/combined/regression/game_players.csv"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=35,
        help="Minimum games required to include a player in the regression output (default: 35)",
    )
    parser.add_argument(
        "--ridge-alpha",
        type=float,
        default=1.0,
        help="Ridge regularization strength for player coefficients",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=None,
        help="Maximum number of players to include in output",
    )
    return parser


def run_regression(
    games_csv_path: Path | str,
    game_players_csv_path: Path | str,
    min_games: int,
    ridge_alpha: float = 1.0,
    top_n: int | None = None,
    teams: list[str] | None = None,
    seasons: list[str] | None = None,
) -> str:
    if min_games < 0:
        raise ValueError("Minimum games filter must be non-negative")
    if ridge_alpha < 0:
        raise ValueError("Ridge alpha must be non-negative")
    if top_n is not None and top_n < 0:
        raise ValueError("Top-n filter must be non-negative")

    games = load_normalized_games_from_csv(games_csv_path)
    game_players = load_normalized_game_players_from_csv(game_players_csv_path)
    games, game_players = filter_regression_scope(
        games,
        game_players,
        teams=teams,
        seasons=seasons,
    )
    observations, player_names = build_regression_observations(games, game_players)
    result = fit_player_regression(
        observations,
        player_names=player_names,
        min_games=min_games,
        ridge_alpha=ridge_alpha,
    )
    return format_regression_results(result, top_n=top_n)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    games_csv = args.games_csv
    game_players_csv = args.game_players_csv
    print(f"[1/3] preparing regression inputs for {format_scope(args.team, args.season)}")
    if games_csv is None or game_players_csv is None:
        games_csv, game_players_csv = prepare_regression_inputs(
            teams=args.team,
            seasons=args.season,
            combined_games_csv=args.combined_games_csv,
            combined_game_players_csv=args.combined_game_players_csv,
            season_type=args.season_type,
            source_data_dir=args.source_data_dir,
            normalized_games_input_dir=args.normalized_games_input_dir,
            normalized_game_players_input_dir=args.normalized_game_players_input_dir,
            wowy_output_dir=args.wowy_output_dir,
        )
    print(f"[2/3] loading regression data from {games_csv} and {game_players_csv}")
    print("[3/3] fitting regression model")
    print(
        run_regression(
            games_csv,
            game_players_csv,
            min_games=args.min_games,
            ridge_alpha=args.ridge_alpha,
            top_n=args.top_n,
            teams=args.team,
            seasons=args.season,
        )
    )
    return 0


def filter_regression_scope(
    games,
    game_players,
    teams: list[str] | None,
    seasons: list[str] | None,
):
    if not teams and not seasons:
        return games, game_players

    normalized_teams = {team.upper() for team in teams or []}
    normalized_seasons = set(seasons or [])
    selected_game_ids = {
        game.game_id
        for game in games
        if (not normalized_seasons or game.season in normalized_seasons)
        and (not normalized_teams or game.team in normalized_teams)
    }
    if not selected_game_ids:
        raise ValueError("No games matched the requested regression scope")

    filtered_games = [game for game in games if game.game_id in selected_game_ids]
    filtered_game_players = [
        player for player in game_players if player.game_id in selected_game_ids
    ]
    return filtered_games, filtered_game_players


if __name__ == "__main__":
    raise SystemExit(main())
