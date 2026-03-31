from __future__ import annotations

from collections.abc import Iterable

from rawr_analytics.nba.models import NormalizedGamePlayerRecord

MinuteStats = dict[int, tuple[float, float]]


def _build_player_minute_stats(
    game_players: Iterable[NormalizedGamePlayerRecord],
) -> MinuteStats:
    totals: dict[int, float] = {}
    counts: dict[int, int] = {}

    for player in game_players:
        if not player.appeared or player.minutes is None or player.minutes <= 0.0:
            continue
        totals[player.player_id] = totals.get(player.player_id, 0.0) + player.minutes
        counts[player.player_id] = counts.get(player.player_id, 0) + 1

    return {
        player_id: (totals[player_id] / counts[player_id], totals[player_id])
        for player_id in totals
    }


def _passes_minute_filters(
    minute_stats: tuple[float, float] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> bool:
    if minute_stats is None:
        return False

    average_minutes, total_minutes = minute_stats
    if min_average_minutes is not None and average_minutes < min_average_minutes:
        return False
    if min_total_minutes is not None and total_minutes < min_total_minutes:
        return False
    return True
