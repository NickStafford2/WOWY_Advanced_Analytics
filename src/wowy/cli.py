from __future__ import annotations

import argparse
from pathlib import Path

from wowy.analysis import compute_wowy, filter_results
from wowy.cache_pipeline import prepare_wowy_inputs
from wowy.formatting import format_results_table
from wowy.ingest_nba import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
    load_player_names_from_cache,
)
from wowy.io import load_games_from_csv
from wowy.types import WowyGameRecord


def format_scope(teams: list[str] | None, seasons: list[str] | None) -> str:
    team_label = ",".join(team.upper() for team in teams) if teams else "all cached teams"
    season_label = ",".join(seasons) if seasons else "all cached seasons"
    return f"teams={team_label} seasons={season_label}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run WOWY on cached data, fetching missing requested scope when needed."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Optional explicit WOWY games CSV path",
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
        "--combined-wowy-csv",
        type=Path,
        default=Path("data/combined/wowy/games.csv"),
        help=argparse.SUPPRESS,
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
    print(f"[1/3] preparing WOWY inputs for {format_scope(args.team, args.season)}")
    if args.csv is not None:
        csv_path = args.csv
        print(f"[2/3] loading WOWY games from {csv_path}")
        player_names = load_player_names_from_cache(args.source_data_dir)
    else:
        csv_path, player_names = prepare_wowy_inputs(
            teams=args.team,
            seasons=args.season,
            combined_wowy_csv=args.combined_wowy_csv,
            season_type=args.season_type,
            source_data_dir=args.source_data_dir,
            normalized_games_input_dir=args.normalized_games_input_dir,
            normalized_game_players_input_dir=args.normalized_game_players_input_dir,
            wowy_output_dir=args.wowy_output_dir,
        )
        print(f"[2/3] running WOWY from {csv_path}")
    print("[3/3] computing WOWY results")
    print(
        run_wowy(
            csv_path,
            min_games_with=args.min_games_with,
            min_games_without=args.min_games_without,
            player_names=player_names,
            top_n=args.top_n,
        )
    )
    return 0
