from __future__ import annotations

import argparse
from pathlib import Path

from wowy.derive_wowy import derive_wowy_games, write_wowy_games_csv
from wowy.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Derive legacy WOWY games.csv from normalized game-level tables."
    )
    parser.add_argument(
        "--games-csv",
        type=Path,
        default=Path("data/normalized/games.csv"),
        help="Path to normalized games CSV",
    )
    parser.add_argument(
        "--game-players-csv",
        type=Path,
        default=Path("data/normalized/game_players.csv"),
        help="Path to normalized game-player CSV",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/combined/wowy/games.csv"),
        help="Output WOWY games CSV path",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    games = load_normalized_games_from_csv(args.games_csv)
    game_players = load_normalized_game_players_from_csv(args.game_players_csv)
    derived_games = derive_wowy_games(games, game_players)
    write_wowy_games_csv(args.output, derived_games)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
