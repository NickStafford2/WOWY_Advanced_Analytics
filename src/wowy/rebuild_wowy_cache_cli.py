from __future__ import annotations

import argparse
from pathlib import Path

from wowy.derive_wowy import derive_wowy_games, write_wowy_games_csv
from wowy.ingest_nba import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)
from wowy.normalized_io import (
    load_normalized_game_players_from_csv,
    load_normalized_games_from_csv,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild derived WOWY team-game CSVs from cached normalized team-season files."
    )
    parser.add_argument(
        "--normalized-games-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAMES_DIR,
        help="Directory containing normalized games CSV files",
    )
    parser.add_argument(
        "--normalized-game-players-input-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
        help="Directory containing normalized game-player CSV files",
    )
    parser.add_argument(
        "--wowy-output-dir",
        type=Path,
        default=DEFAULT_WOWY_GAMES_DIR,
        help="Directory to write rebuilt WOWY team-game CSV files",
    )
    return parser


def rebuild_wowy_cache(
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
) -> int:
    games_paths = sorted(normalized_games_input_dir.glob("*.csv"))
    if not games_paths:
        raise ValueError(
            f"No normalized games CSV files found in {normalized_games_input_dir}"
        )

    wowy_output_dir.mkdir(parents=True, exist_ok=True)
    for existing_path in wowy_output_dir.glob("*.csv"):
        existing_path.unlink()

    rebuilt = 0
    for games_path in games_paths:
        game_players_path = normalized_game_players_input_dir / games_path.name
        if not game_players_path.exists():
            raise ValueError(
                f"Missing normalized game-player CSV for {games_path.name}: {game_players_path}"
            )

        games = load_normalized_games_from_csv(games_path)
        game_players = load_normalized_game_players_from_csv(game_players_path)
        derived_games = derive_wowy_games(games, game_players)
        write_wowy_games_csv(wowy_output_dir / games_path.name, derived_games)
        rebuilt += 1

    return rebuilt


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    rebuilt = rebuild_wowy_cache(
        normalized_games_input_dir=args.normalized_games_input_dir,
        normalized_game_players_input_dir=args.normalized_game_players_input_dir,
        wowy_output_dir=args.wowy_output_dir,
    )
    print(f"Rebuilt {rebuilt} WOWY team-game CSV files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
