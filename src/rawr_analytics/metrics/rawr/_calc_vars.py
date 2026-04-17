from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.metrics.rawr.calculate.shrinkage import RawrShrinkageMode
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_MIN_GAMES,
    DEFAULT_RAWR_RIDGE_ALPHA,
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
)
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class RawrEligibility:
    min_games: int = DEFAULT_RAWR_MIN_GAMES


@dataclass(frozen=True)
class RawrParams:
    teams: list[Team]
    seasons: list[Season]
    eligibility: RawrEligibility
    ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA
    shrinkage_mode: RawrShrinkageMode = DEFAULT_RAWR_SHRINKAGE_MODE
    shrinkage_strength: float = DEFAULT_RAWR_SHRINKAGE_STRENGTH
    shrinkage_minute_scale: float = DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE
