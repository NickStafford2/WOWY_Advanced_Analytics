from __future__ import annotations

from rawr_analytics.metrics.wowy.records import WowyPlayerSeasonRecord


def format_results_table(
    records: list[WowyPlayerSeasonRecord],
    *,
    top_n: int | None = None,
) -> str:
    ranked = records[:top_n] if top_n is not None else list(records)
    if not ranked:
        return "WOWY results (Version 1)\nNo players matched the current filters."

    name_width = max(len("player"), *(len(record.player.player_name) for record in ranked))
    player_id_width = max(
        len("player_id"),
        *(len(str(record.player.player_id)) for record in ranked),
    )
    avg_minutes_width = max(
        len("avg_min"),
        *(len(_format_minutes_value(record.minutes.average_minutes)) for record in ranked),
    )
    total_minutes_width = max(
        len("tot_min"),
        *(len(_format_minutes_value(record.minutes.total_minutes)) for record in ranked),
    )

    lines = [
        "WOWY results (Version 1)",
        "-" * (name_width + player_id_width + avg_minutes_width + total_minutes_width + 59),
        (
            f"{'player':<{name_width}} {'player_id':<{player_id_width}} "
            f"{'avg_min':>{avg_minutes_width}} {'tot_min':>{total_minutes_width}} "
            f"{'with':>6} {'without':>8} {'avg_with':>12} {'avg_without':>14} {'score':>10}"
        ),
        "-" * (name_width + player_id_width + avg_minutes_width + total_minutes_width + 59),
    ]
    for record in ranked:
        lines.append(
            f"{record.player.player_name:<{name_width}} "
            f"{record.player.player_id:<{player_id_width}} "
            f"{_format_minutes_value(record.minutes.average_minutes):>{avg_minutes_width}} "
            f"{_format_minutes_value(record.minutes.total_minutes):>{total_minutes_width}} "
            f"{record.result.games_with:>6} "
            f"{record.result.games_without:>8} "
            f"{record.result.avg_margin_with:>12.2f} "
            f"{record.result.avg_margin_without:>14.2f} "
            f"{record.result.value:>10.2f}"
        )
    return "\n".join(lines)


def _format_minutes_value(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}"
