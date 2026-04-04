from __future__ import annotations

from rawr_analytics.metrics.rawr.records import RawrPlayerSeasonRecord


def format_rawr_records(
    records: list[RawrPlayerSeasonRecord],
    *,
    top_n: int | None = None,
) -> str:
    ranked = records[:top_n] if top_n is not None else list(records)
    if not ranked:
        return "RAWR results (Game-level player model)\nNo players matched the current filters."

    name_width = max(len("player"), *(len(record.player.player_name) for record in ranked))
    season_width = max(len("season"), *(len(str(record.season)) for record in ranked))
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
        "RAWR results (Game-level player model)",
        "-"
        * (
            season_width
            + name_width
            + player_id_width
            + avg_minutes_width
            + total_minutes_width
            + 29
        ),
        (
            f"{'season':<{season_width}} {'player':<{name_width}} {'player_id':<{player_id_width}} "
            f"{'games':>6} {'avg_min':>{avg_minutes_width}} {'tot_min':>{total_minutes_width}} "
            f"{'coef':>10}"
        ),
        "-"
        * (
            season_width
            + name_width
            + player_id_width
            + avg_minutes_width
            + total_minutes_width
            + 29
        ),
    ]
    for record in ranked:
        lines.append(
            f"{record.season:<{season_width}} "
            f"{record.player.player_name:<{name_width}} "
            f"{record.player.player_id:<{player_id_width}} "
            f"{record.games:>6} "
            f"{_format_minutes_value(record.minutes.average_minutes):>{avg_minutes_width}} "
            f"{_format_minutes_value(record.minutes.total_minutes):>{total_minutes_width}} "
            f"{record.coefficient:>10.4f}"
        )
    return "\n".join(lines)


def _format_minutes_value(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}"
