from __future__ import annotations

import argparse
from pathlib import Path

from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)
from wowy.web.app import create_app
from wowy.web.service import RAWR_METRIC, WOWY_METRIC, refresh_metric_store


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the WOWY Flask backend for web development."
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host interface to bind the Flask development server.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port to bind the Flask development server.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable Flask debug mode.",
    )
    parser.add_argument(
        "--refresh-store",
        action="store_true",
        help="Refresh cached web metric stores before starting the server.",
    )
    parser.add_argument(
        "--refresh-metric",
        action="append",
        choices=[WOWY_METRIC, RAWR_METRIC],
        help=(
            "Metric to refresh when used with --refresh-store. "
            "Repeat to refresh multiple metrics. Defaults to refreshing both."
        ),
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to build for the cached leaderboard store.",
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
        "--combined-wowy-csv",
        type=Path,
        default=Path("data/combined/wowy/games.csv"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--combined-rawr-games-csv",
        type=Path,
        default=Path("data/combined/rawr/games.csv"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--combined-rawr-game-players-csv",
        type=Path,
        default=Path("data/combined/rawr/game_players.csv"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--player-metrics-db-path",
        type=Path,
        default=DEFAULT_PLAYER_METRICS_DB_PATH,
        help="SQLite path for the web metric store.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.refresh_store:
        refresh_metrics = args.refresh_metric or [WOWY_METRIC, RAWR_METRIC]
        for metric in refresh_metrics:
            print(f"refreshing {metric} web store at {args.player_metrics_db_path}")
            refresh_metric_store(
                metric,
                season_type=args.season_type,
                db_path=args.player_metrics_db_path,
                source_data_dir=args.source_data_dir,
                normalized_games_input_dir=args.normalized_games_input_dir,
                normalized_game_players_input_dir=args.normalized_game_players_input_dir,
                wowy_output_dir=args.wowy_output_dir,
                combined_wowy_csv=args.combined_wowy_csv,
                combined_rawr_games_csv=args.combined_rawr_games_csv,
                combined_rawr_game_players_csv=args.combined_rawr_game_players_csv,
            )
    app = create_app(
        source_data_dir=args.source_data_dir,
        normalized_games_input_dir=args.normalized_games_input_dir,
        normalized_game_players_input_dir=args.normalized_game_players_input_dir,
        wowy_output_dir=args.wowy_output_dir,
        combined_wowy_csv=args.combined_wowy_csv,
        player_metrics_db_path=args.player_metrics_db_path,
    )
    app.run(host=args.host, port=args.port, debug=args.debug)
    return 0
