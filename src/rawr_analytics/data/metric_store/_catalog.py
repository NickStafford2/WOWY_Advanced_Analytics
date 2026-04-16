from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season, SeasonType, require_normalized_seasons
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class MetricCacheAvailability:
    season_ids: list[str]
    team_ids: list[int]


@dataclass(frozen=True)
class MetricSeasonSpanIds:
    start_season_id: str
    end_season_id: str


@dataclass(frozen=True)
class MetricCacheCatalog:
    label: str
    team_filter: str
    season_type: str
    availability: MetricCacheAvailability
    full_span: MetricSeasonSpanIds | None


@dataclass(frozen=True)
class MetricCacheCatalogRow:
    metric_id: str
    metric_cache_key: str
    label: str
    team_filter: str
    season_type: str
    available_season_ids: list[str]
    available_team_ids: list[int]
    full_span_start_season_id: str | None
    full_span_end_season_id: str | None
    updated_at: str


def build_metric_cache_catalog_row(
    *,
    metric_id: str,
    metric_cache_key: str,
    catalog: MetricCacheCatalog,
    updated_at: str,
) -> MetricCacheCatalogRow:
    return MetricCacheCatalogRow(
        metric_id=metric_id,
        metric_cache_key=metric_cache_key,
        label=catalog.label,
        team_filter=catalog.team_filter,
        season_type=catalog.season_type,
        available_season_ids=catalog.availability.season_ids,
        available_team_ids=catalog.availability.team_ids,
        full_span_start_season_id=(
            None if catalog.full_span is None else catalog.full_span.start_season_id
        ),
        full_span_end_season_id=(
            None if catalog.full_span is None else catalog.full_span.end_season_id
        ),
        updated_at=updated_at,
    )


def build_metric_cache_catalog(
    *,
    label: str,
    team_filter: str,
    season_type: SeasonType,
    seasons: list[Season],
    available_teams: list[Team],
) -> MetricCacheCatalog:
    normalized_seasons = require_normalized_seasons(seasons)
    season_ids = [season.id for season in normalized_seasons]
    return MetricCacheCatalog(
        label=label,
        team_filter=team_filter,
        season_type=season_type.value,
        availability=MetricCacheAvailability(
            season_ids=season_ids,
            team_ids=sorted({team.team_id for team in available_teams}),
        ),
        full_span=MetricSeasonSpanIds(
            start_season_id=season_ids[0],
            end_season_id=season_ids[-1],
        ),
    )


def catalog_seasons(catalog: MetricCacheCatalog | MetricCacheCatalogRow) -> list[Season]:
    season_ids = (
        catalog.availability.season_ids
        if isinstance(catalog, MetricCacheCatalog)
        else catalog.available_season_ids
    )
    seasons = [Season.parse_id(season_id) for season_id in season_ids]
    assert seasons, "metric store catalog requires non-empty seasons"
    return seasons
