from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RawrPlayerSeasonValueRow:
    snapshot_id: int | None
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    season_id: str
    player_id: int
    player_name: str
    games: int
    coefficient: float
    average_minutes: float | None
    total_minutes: float | None


@dataclass(frozen=True)
class WowyPlayerSeasonValueRow:
    snapshot_id: int | None
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    season_id: str
    player_id: int
    player_name: str
    value: float | None
    games_with: int
    games_without: int
    avg_margin_with: float | None
    avg_margin_without: float | None
    average_minutes: float | None
    total_minutes: float | None
    raw_wowy_score: float | None


def metric_values_table(metric_id: str) -> str:
    if metric_id == "rawr":
        return "rawr_player_season_values"
    if metric_id in {"wowy", "wowy_shrunk"}:
        return "wowy_player_season_values"
    raise ValueError(f"Unknown metric table for {metric_id!r}")
