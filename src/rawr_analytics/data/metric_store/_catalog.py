from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season, require_normalized_seasons


@dataclass(frozen=True)
class MetricCacheCatalogRow:
    metric_id: str
    metric_cache_key: str
    season_ids: list[str]
    updated_at: str


def build_metric_cache_catalog_row(
    *,
    metric_id: str,
    metric_cache_key: str,
    seasons: list[Season],
    updated_at: str,
) -> MetricCacheCatalogRow:
    return MetricCacheCatalogRow(
        metric_id=metric_id,
        metric_cache_key=metric_cache_key,
        season_ids=[season.id for season in require_normalized_seasons(seasons)],
        updated_at=updated_at,
    )


def catalog_seasons(catalog: MetricCacheCatalogRow) -> list[Season]:
    seasons = [Season.parse_id(season_id) for season_id in catalog.season_ids]
    assert seasons, "metric store catalog requires non-empty seasons"
    return seasons
