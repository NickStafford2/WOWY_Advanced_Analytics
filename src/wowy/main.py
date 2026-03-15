from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any


def load_games_from_csv(csv_path: Path | str) -> list[dict[str, Any]]:
    games: list[dict[str, Any]] = []

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        required_columns = {"game_id", "team", "margin", "players"}
        missing = required_columns - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

        for row_number, row in enumerate(reader, start=2):
            try:
                margin = float(row["margin"])
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid margin at row {row_number}: {row['margin']!r}"
                ) from exc

            players = {
                player.strip() for player in row["players"].split(";") if player.strip()
            }
            if not players:
                raise ValueError(f"Row {row_number} has no players listed")

            game = {
                "game_id": row["game_id"],
                "team": row["team"],
                "margin": margin,
                "players": players,
            }
            games.append(game)

    return games


def compute_wowy(
    games: list[dict[str, Any]],
) -> dict[str, dict[str, float | int | None]]:
    all_players: set[str] = set()
    for game in games:
        all_players.update(game["players"])

    results: dict[str, dict[str, float | int | None]] = {}

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

        wowy_score = None
        if avg_with is not None and avg_without is not None:
            wowy_score = avg_with - avg_without

        results[player] = {
            "games_with": len(margins_with),
            "games_without": len(margins_without),
            "avg_margin_with": avg_with,
            "avg_margin_without": avg_without,
            "wowy_score": wowy_score,
        }

    return results


def filter_results(
    results: dict[str, dict[str, float | int | None]],
    min_games_with: int = 1,
    min_games_without: int = 1,
) -> dict[str, dict[str, float | int | None]]:
    filtered: dict[str, dict[str, float | int | None]] = {}

    for player, stats in results.items():
        if stats["games_with"] < min_games_with:
            continue
        if stats["games_without"] < min_games_without:
            continue
        if stats["wowy_score"] is None:
            continue
        filtered[player] = stats

    return filtered


def format_results_table(results: dict[str, dict[str, float | int | None]]) -> str:
    lines = [
        "WOWY results (Version 1)",
        "-" * 72,
        (
            f"{'player':<12} {'with':>6} {'without':>8} "
            f"{'avg_with':>12} {'avg_without':>14} {'score':>10}"
        ),
        "-" * 72,
    ]

    ranked = sorted(
        results.items(),
        key=lambda item: item[1]["wowy_score"],
        reverse=True,
    )

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


def print_results(results: dict[str, dict[str, float | int | None]]) -> None:
    print(format_results_table(results))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute a simple game-level WOWY score from a CSV file."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path(__file__).resolve().parent / "games.csv",
        help="Path to the games CSV file",
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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.min_games_with < 0 or args.min_games_without < 0:
        raise ValueError("Minimum game filters must be non-negative")

    games = load_games_from_csv(args.csv)
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
