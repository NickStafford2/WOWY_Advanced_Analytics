from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary


@dataclass(frozen=True)
class PlayerSeasonFilters:
    min_average_minutes: float | None
    min_total_minutes: float | None


@dataclass(frozen=True)
class PlayerSeasonContext:
    player: PlayerSummary
    minutes: PlayerMinutes

    def passes_minute_filters(
        self,
        filters: PlayerSeasonFilters,
    ) -> bool:
        if filters.min_average_minutes is not None and (
            self.minutes.average_minutes is None
            or self.minutes.average_minutes < filters.min_average_minutes
        ):
            return False
        return filters.min_total_minutes is None or (
            self.minutes.total_minutes is not None
            and self.minutes.total_minutes >= filters.min_total_minutes
        )
