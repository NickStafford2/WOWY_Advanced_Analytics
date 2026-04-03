from __future__ import annotations

from rawr_analytics.basketball.models import NormalizedGamePlayerRecord


def has_positive_minutes(minutes: float | None) -> bool:
    return minutes is not None and minutes > 0.0


def player_has_positive_minutes(player: NormalizedGamePlayerRecord) -> bool:
    return player.appeared and has_positive_minutes(player.minutes)


__all__ = [
    "has_positive_minutes",
    "player_has_positive_minutes",
]
