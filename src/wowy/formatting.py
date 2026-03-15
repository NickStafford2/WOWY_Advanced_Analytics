from __future__ import annotations

from wowy.types import PlayerStats


def sort_score(item: tuple[str, PlayerStats]) -> float:
    score = item[1]["wowy_score"]
    if score is None:
        raise ValueError("format_results_table received an unscored player")
    return score


def format_results_table(results: dict[str, PlayerStats]) -> str:
    lines = [
        "WOWY results (Version 1)",
        "-" * 72,
        (
            f"{'player':<12} {'with':>6} {'without':>8} "
            f"{'avg_with':>12} {'avg_without':>14} {'score':>10}"
        ),
        "-" * 72,
    ]

    ranked = sorted(results.items(), key=sort_score, reverse=True)

    for player, stats in ranked:
        avg_margin_with = stats["avg_margin_with"]
        avg_margin_without = stats["avg_margin_without"]
        wowy_score = stats["wowy_score"]

        if avg_margin_with is None or avg_margin_without is None or wowy_score is None:
            raise ValueError("format_results_table received incomplete player stats")

        lines.append(
            f"{player:<12} "
            f"{stats['games_with']:>6} "
            f"{stats['games_without']:>8} "
            f"{avg_margin_with:>12.2f} "
            f"{avg_margin_without:>14.2f} "
            f"{wowy_score:>10.2f}"
        )

    return "\n".join(lines)


def print_results(results: dict[str, PlayerStats]) -> None:
    print(format_results_table(results))
