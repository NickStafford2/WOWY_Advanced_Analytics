from __future__ import annotations

import argparse
from pathlib import Path

from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.cache_migration import normalize_cache_season_keys
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize cached NBA season keys to canonical YYYY-YY format."
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
        "--player-metrics-db-path",
        type=Path,
        default=DEFAULT_PLAYER_METRICS_DB_PATH,
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    summary = normalize_cache_season_keys(
        source_data_dir=args.source_data_dir,
        normalized_games_input_dir=args.normalized_games_input_dir,
        normalized_game_players_input_dir=args.normalized_game_players_input_dir,
        wowy_output_dir=args.wowy_output_dir,
        combined_wowy_csv=args.combined_wowy_csv,
        combined_rawr_games_csv=args.combined_rawr_games_csv,
        player_metrics_db_path=args.player_metrics_db_path,
    )
    print(
        "normalized cache seasons: "
        f"renamed_files={summary.renamed_files} "
        f"rewritten_files={summary.rewritten_files} "
        f"updated_db_rows={summary.updated_db_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
