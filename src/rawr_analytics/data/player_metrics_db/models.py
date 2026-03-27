from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PlayerSeasonMetricRow:
    metric: str
    metric_label: str
    scope_key: str
    team_filter: str
    season_type: str
    season: str
    player_id: int
    player_name: str
    value: float
    sample_size: int | None = None
    secondary_sample_size: int | None = None
    average_minutes: float | None = None
    total_minutes: float | None = None
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class MetricStoreMetadata:
    metric: str
    scope_key: str
    metric_label: str
    build_version: str
    source_fingerprint: str
    row_count: int
    updated_at: str


@dataclass(frozen=True)
class MetricScopeCatalogRow:
    metric: str
    scope_key: str
    metric_label: str
    team_filter: str
    season_type: str
    available_seasons: list[str]
    available_team_ids: list[int]
    full_span_start_season: str | None
    full_span_end_season: str | None
    updated_at: str


@dataclass(frozen=True)
class MetricFullSpanSeriesRow:
    metric: str
    scope_key: str
    player_id: int
    player_name: str
    span_average_value: float
    season_count: int
    rank_order: int


@dataclass(frozen=True)
class MetricFullSpanPointRow:
    metric: str
    scope_key: str
    player_id: int
    season: str
    value: float
