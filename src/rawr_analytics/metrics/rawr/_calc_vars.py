from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrEligibility:
    min_games: int


@dataclass(frozen=True)
class RawrCalcVars:
    teams: list[Team]
    seasons: list[Season]
    eligibility: RawrEligibility
    ridge_alpha: float
    shrinkage_mode: RawrShrinkageMode
    shrinkage_strength: float
    shrinkage_minute_scale: float
