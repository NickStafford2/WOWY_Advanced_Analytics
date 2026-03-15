from __future__ import annotations

from wowy.types import GameRecord, PlayerStats


def compute_wowy(games: list[GameRecord]) -> dict[int, PlayerStats]:
    all_players: set[int] = set()
    for game in games:
        all_players.update(game["players"])

    results: dict[int, PlayerStats] = {}

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

        wowy_score: float | None = None
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
    results: dict[int, PlayerStats],
    min_games_with: int = 1,
    min_games_without: int = 1,
) -> dict[int, PlayerStats]:
    filtered: dict[int, PlayerStats] = {}

    for player, stats in results.items():
        if stats["games_with"] < min_games_with:
            continue
        if stats["games_without"] < min_games_without:
            continue
        if stats["wowy_score"] is None:
            continue
        filtered[player] = stats

    return filtered
