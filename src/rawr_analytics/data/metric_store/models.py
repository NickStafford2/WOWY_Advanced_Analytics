from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PlayerSeasonMetricRow:
    metric_id: str
    scope_key: str
    team_filter: str
    season_type: str
    season_id: str
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
    metric_id: str
    scope_key: str
    label: str
    build_version: str
    source_fingerprint: str
    row_count: int
    updated_at: str


@dataclass(frozen=True)
class MetricScopeCatalogRow:
    metric_id: str
    scope_key: str
    label: str
    team_filter: str
    season_type: str
    available_season_ids: list[str]
    available_team_ids: list[int]
    full_span_start_season_id: str | None
    full_span_end_season_id: str | None
    updated_at: str


@dataclass(frozen=True)
class MetricFullSpanSeriesRow:
    metric_id: str
    scope_key: str
    player_id: int
    player_name: str
    span_average_value: float
    season_count: int
    rank_order: int


@dataclass(frozen=True)
class MetricFullSpanPointRow:
    metric_id: str
    scope_key: str
    player_id: int
    season_id: str
    value: float
