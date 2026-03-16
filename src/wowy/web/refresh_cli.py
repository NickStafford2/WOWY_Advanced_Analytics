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
from wowy.web.service import WOWY_METRIC, refresh_metric_store


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the SQLite metric store used by the web app."
    )
    parser.add_argument(
        "--metric",
        default=WOWY_METRIC,
        choices=[WOWY_METRIC],
        help="Metric to refresh into the SQLite store.",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to build.",
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
        "--player-metrics-db-path",
        type=Path,
        default=DEFAULT_PLAYER_METRICS_DB_PATH,
        help="SQLite path for the web metric store.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    refresh_metric_store(
        args.metric,
        season_type=args.season_type,
        db_path=args.player_metrics_db_path,
        source_data_dir=args.source_data_dir,
        normalized_games_input_dir=args.normalized_games_input_dir,
        normalized_game_players_input_dir=args.normalized_game_players_input_dir,
        wowy_output_dir=args.wowy_output_dir,
        combined_wowy_csv=args.combined_wowy_csv,
    )
    print(f"refreshed {args.metric} store at {args.player_metrics_db_path}")
    return 0
