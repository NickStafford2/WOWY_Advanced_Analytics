from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

from rawr_analytics.metrics.rawr._calc_vars import RawrParams
from rawr_analytics.metrics.wowy._calc_vars import WowyParams
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

    @classmethod
    def parse(cls, value: str) -> MetricCacheKey:
        metric_id = ""
        metric_variant = ""
        season_ids: list[str] = []
        team_ids: list[int] = []
        calc_settings: list[tuple[str, str]] = []
        for part in value.split("|"):
            key, separator, raw_value = part.partition("=")
            if separator != "=":
                raise ValueError(f"Invalid metric cache key part {part!r}")
            if key == "metric":
                metric_id = raw_value
                continue
            if key == "variant":
                metric_variant = raw_value
                continue
            if key == "team_ids":
                if raw_value == "all-teams":
                    team_ids = []
                else:
                    team_ids = [int(team_id) for team_id in raw_value.split(",") if team_id]
                continue
            if key == "season_ids":
                season_ids = [season_id for season_id in raw_value.split(",") if season_id]
                continue
            calc_settings.append((key, raw_value))
        if not metric_id:
            raise ValueError("Metric cache key is missing metric id")
        if not metric_variant:
            raise ValueError("Metric cache key is missing metric variant")
        if not season_ids:
            raise ValueError("Metric cache key is missing season ids")
        return cls(
            metric_id=metric_id,
            metric_variant=metric_variant,
            season_ids=season_ids,
            team_ids=team_ids,
            calc_settings=tuple(calc_settings),
        )


def build_rawr_metric_cache_key(calc_vars: RawrParams) -> str:
    return MetricCacheKey(
        metric_id="rawr",
        metric_variant="default",
        season_ids=_season_ids(calc_vars.seasons),
        team_ids=_team_ids(calc_vars.teams),
        calc_settings=(
            ("ridge_alpha", _normalize_float(calc_vars.ridge_alpha)),
            ("shrinkage_mode", calc_vars.shrinkage_mode.value),
            ("shrinkage_strength", _normalize_float(calc_vars.shrinkage_strength)),
            ("shrinkage_minute_scale", _normalize_float(calc_vars.shrinkage_minute_scale)),
        ),
    ).serialize()


def build_wowy_metric_cache_key(
    *,
    metric_id: str,
    calc_vars: WowyParams,
) -> str:
    calc_settings: tuple[tuple[str, str], ...] = ()
    if metric_id == "wowy_shrunk" and calc_vars.shrinkage_prior_games is not None:
        calc_settings = (
            ("shrinkage_prior_games", _normalize_float(calc_vars.shrinkage_prior_games)),
        )
    return MetricCacheKey(
        metric_id=metric_id,
        metric_variant=metric_id,
        season_ids=_season_ids(calc_vars.seasons),
        team_ids=_team_ids(calc_vars.teams),
        calc_settings=calc_settings,
    ).serialize()


def _season_ids(seasons) -> list[str]:
    return [season.id for season in require_normalized_seasons(seasons)]


def _team_ids(teams: list[Team]) -> list[int]:
    return to_normalized_team_ids(teams) or []


def _normalize_float(value: float) -> str:
    quantized = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{quantized:.2f}"
