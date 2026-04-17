from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class WowyEligibility:
    min_games_with: int
    min_games_without: int


@dataclass(frozen=True)
class WowyParams:
    teams: list[Team]
    seasons: list[Season]
    eligibility: WowyEligibility
    shrinkage_prior_games: float | None
