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
from wowy.progress import TerminalProgressBar, print_status_box
from wowy.web.service import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    RAWR_METRIC,
    WOWY_METRIC,
    refresh_metric_store,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the SQLite metric store used by the web app."
    )
    parser.add_argument(
        "--metric",
        default=WOWY_METRIC,
        choices=[WOWY_METRIC, RAWR_METRIC],
        help="Metric to refresh into the SQLite store.",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to build.",
    )
    parser.add_argument(
        "--rawr-ridge-alpha",
        type=float,
        default=DEFAULT_RAWR_RIDGE_ALPHA,
        help="Ridge alpha used when building cached RAWR web rows.",
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
    print_status_box(
        "Web Store Refresh",
        [
            f"Metric: {args.metric}",
            "Refreshing cached player-season rows and full-span leaderboard"
            " slices used by the Flask and React web app.",
            "The progress bar below tracks each built team scope in the SQLite"
            " metric store.",
        ],
    )
    progress_bar = TerminalProgressBar("Refresh", total=1)
    refresh_metric_store(
        args.metric,
        season_type=args.season_type,
        db_path=args.player_metrics_db_path,
        source_data_dir=args.source_data_dir,
        normalized_games_input_dir=args.normalized_games_input_dir,
        normalized_game_players_input_dir=args.normalized_game_players_input_dir,
        wowy_output_dir=args.wowy_output_dir,
        combined_wowy_csv=args.combined_wowy_csv,
        combined_rawr_games_csv=args.combined_rawr_games_csv,
        combined_rawr_game_players_csv=args.combined_rawr_game_players_csv,
        rawr_ridge_alpha=args.rawr_ridge_alpha,
        progress=lambda current, total, detail: _update_progress(
            progress_bar,
            current=current,
            total=total,
            detail=detail,
        ),
    )
    progress_bar.finish(detail="done")
    print(f"refreshed {args.metric} store at {args.player_metrics_db_path}")
    return 0


def _update_progress(
    progress_bar: TerminalProgressBar,
    *,
    current: int,
    total: int,
    detail: str,
) -> None:
    progress_bar.total = max(total, 1)
    progress_bar.update(current, detail=detail)


if __name__ == "__main__":
    raise SystemExit(main())
