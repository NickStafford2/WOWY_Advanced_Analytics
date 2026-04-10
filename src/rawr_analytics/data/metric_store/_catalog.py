from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.shared.season import Season, SeasonType, require_normalized_seasons
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class MetricScopeAvailability:
    season_ids: list[str]
    team_ids: list[int]


@dataclass(frozen=True)
class MetricSeasonSpanIds:
    start_season_id: str
    end_season_id: str


@dataclass(frozen=True)
class MetricScopeCatalog:
    label: str
    team_filter: str
    season_type: str
    availability: MetricScopeAvailability
    full_span: MetricSeasonSpanIds | None


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


def build_metric_scope_catalog_row(
    *,
    metric_id: str,
    scope_key: str,
    catalog: MetricScopeCatalog,
    updated_at: str,
) -> MetricScopeCatalogRow:
    return MetricScopeCatalogRow(
        metric_id=metric_id,
        scope_key=scope_key,
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


def build_metric_scope_catalog(
    *,
    label: str,
    team_filter: str,
    season_type: SeasonType,
    seasons: list[Season],
    available_teams: list[Team],
) -> MetricScopeCatalog:
    normalized_seasons = require_normalized_seasons(seasons)
    season_ids = [season.year_string_nba_api for season in normalized_seasons]
    return MetricScopeCatalog(
        label=label,
        team_filter=team_filter,
        season_type=season_type.value,
        availability=MetricScopeAvailability(
            season_ids=season_ids,
            team_ids=sorted({team.team_id for team in available_teams}),
        ),
        full_span=MetricSeasonSpanIds(
            start_season_id=season_ids[0],
            end_season_id=season_ids[-1],
        ),
    )


def catalog_seasons(catalog: MetricScopeCatalog | MetricScopeCatalogRow) -> list[Season]:
    season_type = SeasonType.parse(catalog.season_type)
    season_ids = (
        catalog.availability.season_ids
        if isinstance(catalog, MetricScopeCatalog)
        else catalog.available_season_ids
    )
    seasons = [Season.parse(season_id, season_type.value) for season_id in season_ids]
    assert seasons, "metric store catalog requires non-empty seasons"
    return seasons
