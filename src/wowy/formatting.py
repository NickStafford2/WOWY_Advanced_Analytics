from __future__ import annotations

from wowy.types import PlayerStats


def sort_score(item: tuple[int, PlayerStats]) -> float:
    score = item[1]["wowy_score"]
    if score is None:
        raise ValueError("format_results_table received an unscored player")
    return score


def format_results_table(
    results: dict[int, PlayerStats],
    player_names: dict[int, str] | None = None,
) -> str:
    ranked = sorted(results.items(), key=sort_score, reverse=True)
    player_names = player_names or {}
    name_width = max(
        len("player"),
        *(len(player_names.get(player, str(player))) for player, _ in ranked),
    )
    player_id_width = max(len("player_id"), *(len(str(player)) for player, _ in ranked))

    lines = [
        "WOWY results (Version 1)",
        "-" * (name_width + player_id_width + 55),
        (
            f"{'player':<{name_width}} {'player_id':<{player_id_width}} "
            f"{'with':>6} {'without':>8} "
            f"{'avg_with':>12} {'avg_without':>14} {'score':>10}"
        ),
        "-" * (name_width + player_id_width + 55),
    ]

    for player, stats in ranked:
        player_name = player_names.get(player, str(player))
        player_text = str(player)
        avg_margin_with = stats["avg_margin_with"]
        avg_margin_without = stats["avg_margin_without"]
        wowy_score = stats["wowy_score"]

        if avg_margin_with is None or avg_margin_without is None or wowy_score is None:
            raise ValueError("format_results_table received incomplete player stats")

        lines.append(
            f"{player_name:<{name_width}} "
            f"{player_text:<{player_id_width}} "
            f"{stats['games_with']:>6} "
            f"{stats['games_without']:>8} "
            f"{avg_margin_with:>12.2f} "
            f"{avg_margin_without:>14.2f} "
            f"{wowy_score:>10.2f}"
        )

    return "\n".join(lines)


def print_results(
    results: dict[int, PlayerStats],
    player_names: dict[int, str] | None = None,
) -> None:
    print(format_results_table(results, player_names=player_names))
