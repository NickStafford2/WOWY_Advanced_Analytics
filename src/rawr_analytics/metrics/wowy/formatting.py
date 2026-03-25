from __future__ import annotations

from wowy.metrics.wowy.models import WowyPlayerStats


def sort_score(item: tuple[int, WowyPlayerStats]) -> float:
    score = item[1].wowy_score
    if score is None:
        raise ValueError("format_results_table received an unscored player")
    return score


def format_results_table(
    results: dict[int, WowyPlayerStats],
    player_names: dict[int, str] | None = None,
    top_n: int | None = None,
) -> str:
    ranked = sorted(results.items(), key=sort_score, reverse=True)
    if top_n is not None:
        ranked = ranked[:top_n]
    if not ranked:
        return "WOWY results (Version 1)\nNo players matched the current filters."
    player_names = player_names or {}
    name_width = max(
        len("player"),
        *(len(player_names.get(player, str(player))) for player, _ in ranked),
    )
    player_id_width = max(len("player_id"), *(len(str(player)) for player, _ in ranked))
    avg_minutes_width = max(
        len("avg_min"),
        *(len(format_minutes_value(stats.average_minutes)) for _, stats in ranked),
    )
    total_minutes_width = max(
        len("tot_min"),
        *(len(format_minutes_value(stats.total_minutes)) for _, stats in ranked),
    )

    lines = [
        "WOWY results (Version 1)",
        "-" * (name_width + player_id_width + avg_minutes_width + total_minutes_width + 59),
        (
            f"{'player':<{name_width}} {'player_id':<{player_id_width}} "
            f"{'avg_min':>{avg_minutes_width}} {'tot_min':>{total_minutes_width}} "
            f"{'with':>6} {'without':>8} "
            f"{'avg_with':>12} {'avg_without':>14} {'score':>10}"
        ),
        "-" * (name_width + player_id_width + avg_minutes_width + total_minutes_width + 59),
    ]

    for player, stats in ranked:
        player_name = player_names.get(player, str(player))
        player_text = str(player)
        avg_margin_with = stats.avg_margin_with
        avg_margin_without = stats.avg_margin_without
        wowy_score = stats.wowy_score

        if avg_margin_with is None or avg_margin_without is None or wowy_score is None:
            raise ValueError("format_results_table received incomplete player stats")

        lines.append(
            f"{player_name:<{name_width}} "
            f"{player_text:<{player_id_width}} "
            f"{format_minutes_value(stats.average_minutes):>{avg_minutes_width}} "
            f"{format_minutes_value(stats.total_minutes):>{total_minutes_width}} "
            f"{stats.games_with:>6} "
            f"{stats.games_without:>8} "
            f"{avg_margin_with:>12.2f} "
            f"{avg_margin_without:>14.2f} "
            f"{wowy_score:>10.2f}"
        )

    return "\n".join(lines)


def print_results(
    results: dict[int, WowyPlayerStats],
    player_names: dict[int, str] | None = None,
    top_n: int | None = None,
) -> None:
    print(format_results_table(results, player_names=player_names, top_n=top_n))


def format_minutes_value(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}"
