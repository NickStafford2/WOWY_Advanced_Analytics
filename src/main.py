from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any


Game = dict[str, Any]
Stats = dict[str, float | int | None]
Results = dict[str, Stats]

REQUIRED_COLUMNS = {"game_id", "team", "margin", "players"}
DEFAULT_MIN_GAMES_WITH = 2
DEFAULT_MIN_GAMES_WITHOUT = 2


def parse_players(raw_players: str) -> set[str]:
    return {player.strip() for player in raw_players.split(";") if player.strip()}


def validate_csv_columns(fieldnames: list[str] | None) -> None:
    missing = REQUIRED_COLUMNS - set(fieldnames or [])
    if missing:
        raise ValueError(f"Missing required CSV columns: {sorted(missing)}")


def load_games_from_csv(csv_path: str | Path) -> list[Game]:
    games: list[Game] = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        validate_csv_columns(reader.fieldnames)

        for row_number, row in enumerate(reader, start=2):
            players = parse_players(row["players"])
            if not players:
                raise ValueError(
                    f"Row {row_number}: players column must contain at least one player"
                )

            try:
                margin = float(row["margin"])
            except ValueError as exc:
                raise ValueError(
                    f"Row {row_number}: margin must be numeric, got {row['margin']!r}"
                ) from exc

            game: Game = {
                "game_id": row["game_id"],
                "team": row["team"],
                "margin": margin,
                "players": players,
            }
            games.append(game)

    return games


def compute_wowy(games: list[Game]) -> Results:
    all_players: set[str] = set()
    for game in games:
        all_players.update(game["players"])

    results: Results = {}

    for player in sorted(all_players):
        margins_with: list[float] = []
        margins_without: list[float] = []

        for game in games:
            if player in game["players"]:
                margins_with.append(game["margin"])
            else:
                margins_without.append(game["margin"])

        avg_with = sum(margins_with) / len(margins_with) if margins_with else None
        avg_without = (
            sum(margins_without) / len(margins_without) if margins_without else None
        )
        wowy_score = (
            avg_with - avg_without
            if avg_with is not None and avg_without is not None
            else None
        )

        results[player] = {
            "games_with": len(margins_with),
            "games_without": len(margins_without),
            "avg_margin_with": avg_with,
            "avg_margin_without": avg_without,
            "wowy_score": wowy_score,
        }

    return results


def filter_results(
    results: Results,
    min_games_with: int = 1,
    min_games_without: int = 1,
) -> Results:
    filtered: Results = {}

    for player, stats in results.items():
        if stats["games_with"] < min_games_with:
            continue
        if stats["games_without"] < min_games_without:
            continue
        if stats["wowy_score"] is None:
            continue
        filtered[player] = stats

    return filtered


def rank_results(results: Results) -> list[tuple[str, Stats]]:
    return sorted(
        results.items(),
        key=lambda item: (item[1]["wowy_score"], item[0]),
        reverse=True,
    )


def format_results_table(results: Results) -> str:
    lines = [
        "WOWY results (Version 1)",
        "-" * 72,
        f"{'player':<12} {'with':>6} {'without':>8} {'avg_with':>12} {'avg_without':>14} {'score':>10}",
        "-" * 72,
    ]

    ranked = rank_results(results)
    if not ranked:
        lines.append("No players matched the current filtering rules.")
        return "\n".join(lines)

    for player, stats in ranked:
        lines.append(
            f"{player:<12} "
            f"{stats['games_with']:>6} "
            f"{stats['games_without']:>8} "
            f"{stats['avg_margin_with']:>12.2f} "
            f"{stats['avg_margin_without']:>14.2f} "
            f"{stats['wowy_score']:>10.2f}"
        )

    return "\n".join(lines)


def print_results(results: Results) -> None:
    print(format_results_table(results))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute a simple game-level WOWY score from a games CSV file."
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=Path(__file__).resolve().parent / "games.csv",
        type=Path,
        help="Path to a CSV file with game rows. Defaults to src/games.csv.",
    )
    parser.add_argument(
        "--min-games-with",
        type=int,
        default=DEFAULT_MIN_GAMES_WITH,
        help="Minimum number of games a player must appear in.",
    )
    parser.add_argument(
        "--min-games-without",
        type=int,
        default=DEFAULT_MIN_GAMES_WITHOUT,
        help="Minimum number of games a player must be absent for.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.min_games_with < 0 or args.min_games_without < 0:
        parser.error("minimum game filters must be non-negative")

    games = load_games_from_csv(args.csv_path)
    results = compute_wowy(games)
    filtered_results = filter_results(
        results,
        min_games_with=args.min_games_with,
        min_games_without=args.min_games_without,
    )
    print_results(filtered_results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
