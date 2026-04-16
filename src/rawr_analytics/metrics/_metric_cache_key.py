from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

from rawr_analytics.metrics.rawr.query.request import RawrCalcVars
from rawr_analytics.metrics.wowy.query.request import WowyCalcVars
from rawr_analytics.shared.season import require_normalized_seasons
from rawr_analytics.shared.team import Team, to_normalized_team_ids


@dataclass(frozen=True)
class MetricCacheKey:
    metric_id: str
    metric_variant: str
    season_ids: list[str]
    team_ids: list[int]
    calc_settings: tuple[tuple[str, str], ...]

    def serialize(self) -> str:
        team_part = ",".join(str(team_id) for team_id in self.team_ids) or "all-teams"
        season_part = ",".join(self.season_ids)
        settings_part = "|".join(f"{key}={value}" for key, value in self.calc_settings)
        parts = [
            f"metric={self.metric_id}",
            f"variant={self.metric_variant}",
            f"team_ids={team_part}",
            f"season_ids={season_part}",
        ]
        if settings_part:
            parts.append(settings_part)
        return "|".join(parts)


def build_rawr_metric_cache_key(calc_vars: RawrCalcVars) -> str:
    return MetricCacheKey(
        metric_id="rawr",
        metric_variant="default",
        season_ids=_season_ids(calc_vars.seasons),
        team_ids=_team_ids(calc_vars.teams),
        calc_settings=(("ridge_alpha", _normalize_float(calc_vars.ridge_alpha)),),
    ).serialize()


def build_wowy_metric_cache_key(
    *,
    metric_id: str,
    calc_vars: WowyCalcVars,
) -> str:
    return MetricCacheKey(
        metric_id=metric_id,
        metric_variant=metric_id,
        season_ids=_season_ids(calc_vars.seasons),
        team_ids=_team_ids(calc_vars.teams),
        calc_settings=(),
    ).serialize()


def _season_ids(seasons) -> list[str]:
    return [season.id for season in require_normalized_seasons(seasons)]


def _team_ids(teams: list[Team]) -> list[int]:
    return to_normalized_team_ids(teams) or []


def _normalize_float(value: float) -> str:
    quantized = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{quantized:.2f}"
