from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rawr_analytics.data.game_cache import (
    GameCacheSnapshot,
    load_cache_snapshot,
)
from rawr_analytics.data.metric_store import load_metric_scope_store_state
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import (
    Season,
    SeasonType,
    require_normalized_seasons,
)
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class MetricCatalogAvailability:
    teams: list[Team]
    seasons: list[Season]


@dataclass(frozen=True)
class MetricSeasonSpan:
    start_season: Season
    end_season: Season


@dataclass(frozen=True)
class MetricStoreCatalog:
    season_type: SeasonType
    availability: MetricCatalogAvailability
    full_span: MetricSeasonSpan | None


@dataclass(frozen=True)
class _MetricCatalogAvailabilityIndex:
    season_type: SeasonType
    teams: list[Team]
    seasons: list[Season]
    season_ids_by_team_id: dict[int, list[str]]
    team_ids_by_season_id: dict[str, list[int]]


@dataclass(frozen=True)
class _MetricCatalogOptionsContext:
    catalog: MetricStoreCatalog
    availability_index: _MetricCatalogAvailabilityIndex


class _MetricStoreCatalogUnavailableError(Exception):
    pass


_MISSING_SCOPE_ERROR = "Metric store has not been built for the requested scope"
_EMPTY_CACHE_ERROR = (
    "Normalized cache is empty for the requested scope season type. "
    "Rebuild ingest before using cached metrics."
)
_STALE_SCOPE_ERROR = (
    "Cached metric store is stale relative to normalized cache. "
    "Refresh the web metric store after ingest is rebuilt."
)


def build_metric_options_payload(
    *,
    metric: Metric,
    teams: list[Team] | None,
    season_type: SeasonType,
    filters: dict[str, Any],
) -> dict[str, Any]:
    context = _load_metric_options_context(
        metric=metric,
        teams=teams,
        season_type=season_type,
    )
    return {
        "metric": metric.value,
        "available_teams": [team.current.abbreviation for team in context.availability_index.teams],
        "team_options": _build_team_options(context.availability_index),
        "available_seasons": [
            season.year_string_nba_api for season in context.availability_index.seasons
        ],
        "available_teams_by_season": _build_available_teams_by_season(context.availability_index),
        "filters": filters,
    }


def require_current_metric_scope(
    *,
    metric: Metric,
    scope_key: str,
) -> MetricStoreCatalog:
    try:
        return _load_current_metric_scope_catalog(
            metric=metric,
            scope_key=scope_key,
        )
    except _MetricStoreCatalogUnavailableError as err:
        raise ValueError(str(err)) from None


def resolve_all_teams_snapshot_scope_key(
    *,
    teams: list[Team] | None,
    season_type: SeasonType,
    cache_snapshot: GameCacheSnapshot | None = None,
) -> str | None:
    team_filter = build_team_filter(teams)
    if team_filter:
        return None
    resolved_snapshot = cache_snapshot or load_cache_snapshot(season_type)
    if not resolved_snapshot.entries:
        return None
    seasons = _available_cache_seasons(resolved_snapshot.scopes)
    return build_scope_key(
        seasons=seasons,
        team_filter=team_filter,
    )


def _load_metric_options_context(
    *,
    metric: Metric,
    teams: list[Team] | None,
    season_type: SeasonType,
) -> _MetricCatalogOptionsContext:
    cache_snapshot = _require_cache_snapshot(season_type)
    scope_key = resolve_all_teams_snapshot_scope_key(
        teams=teams,
        season_type=season_type,
        cache_snapshot=cache_snapshot,
    )
    if scope_key is not None:
        try:
            catalog = _load_current_metric_scope_catalog(
                metric=metric,
                scope_key=scope_key,
                cache_snapshot=cache_snapshot,
            )
            availability_index = _build_metric_catalog_availability_index(
                season_type=season_type,
                cached_team_seasons=cache_snapshot.scopes,
                teams=catalog.availability.teams,
                seasons=catalog.availability.seasons,
            )
            return _MetricCatalogOptionsContext(
                catalog=catalog,
                availability_index=availability_index,
            )
        except _MetricStoreCatalogUnavailableError:
            pass

    filtered_cached_team_seasons = _filter_cached_team_seasons(
        cache_snapshot.scopes,
        team_ids=None if teams is None else {team.team_id for team in teams},
    )
    if not filtered_cached_team_seasons:
        raise ValueError(_EMPTY_CACHE_ERROR) from None
    availability_index = _build_metric_catalog_availability_index(
        season_type=season_type,
        cached_team_seasons=filtered_cached_team_seasons,
    )
    catalog = _build_metric_options_catalog_from_index(availability_index)
    return _MetricCatalogOptionsContext(
        catalog=catalog,
        availability_index=availability_index,
    )


def _load_current_metric_scope_catalog(
    *,
    metric: Metric,
    scope_key: str,
    cache_snapshot: GameCacheSnapshot | None = None,
) -> MetricStoreCatalog:
    state = load_metric_scope_store_state(metric.value, scope_key)
    if state is None:
        raise _MetricStoreCatalogUnavailableError(_MISSING_SCOPE_ERROR)

    try:
        season_type = SeasonType.parse(state.catalog_row.season_type)
        if state.catalog_row.season_type != season_type.value:
            raise ValueError("metric store catalog season_type is not canonical")
        catalog = _build_metric_store_catalog_from_store_row(
            season_type=season_type,
            available_team_ids=state.catalog_row.available_team_ids,
            available_season_ids=state.catalog_row.available_season_ids,
            start_season_id=state.catalog_row.full_span_start_season_id,
            end_season_id=state.catalog_row.full_span_end_season_id,
        )
    except (AssertionError, ValueError) as exc:
        raise _MetricStoreCatalogUnavailableError(str(exc)) from None
    resolved_snapshot = cache_snapshot or _require_cache_snapshot(catalog.season_type)
    assert resolved_snapshot.season_type == catalog.season_type, (
        "metric store season type must match the normalized cache snapshot"
    )
    if state.snapshot_state.source_fingerprint != resolved_snapshot.fingerprint:
        raise _MetricStoreCatalogUnavailableError(_STALE_SCOPE_ERROR)
    return catalog


def _build_metric_store_catalog_from_store_row(
    *,
    season_type: SeasonType,
    available_team_ids: list[int],
    available_season_ids: list[str],
    start_season_id: str | None,
    end_season_id: str | None,
) -> MetricStoreCatalog:
    return MetricStoreCatalog(
        season_type=season_type,
        availability=MetricCatalogAvailability(
            teams=[Team.from_id(team_id) for team_id in available_team_ids],
            seasons=_parse_catalog_seasons(
                season_type=season_type,
                season_ids=available_season_ids,
            ),
        ),
        full_span=_build_metric_season_span(
            season_type=season_type,
            start_season_id=start_season_id,
            end_season_id=end_season_id,
        ),
    )


def _build_metric_options_catalog_from_index(
    availability_index: _MetricCatalogAvailabilityIndex,
) -> MetricStoreCatalog:
    seasons = availability_index.seasons
    return MetricStoreCatalog(
        season_type=availability_index.season_type,
        availability=MetricCatalogAvailability(
            teams=availability_index.teams,
            seasons=seasons,
        ),
        full_span=_build_metric_season_span(
            season_type=availability_index.season_type,
            start_season_id=None if not seasons else seasons[0].id,
            end_season_id=None if not seasons else seasons[-1].id,
        ),
    )


def _require_cache_snapshot(season_type: SeasonType) -> GameCacheSnapshot:
    cache_snapshot = load_cache_snapshot(season_type)
    if not cache_snapshot.entries:
        raise ValueError(_EMPTY_CACHE_ERROR)
    return cache_snapshot


def _available_cache_seasons(cached_team_seasons: list[TeamSeasonScope]) -> list[Season]:
    return require_normalized_seasons(
        [team_season.season for team_season in cached_team_seasons]
    )


def _build_metric_catalog_availability_index(
    *,
    season_type: SeasonType,
    cached_team_seasons: list[TeamSeasonScope],
    teams: list[Team] | None = None,
    seasons: list[Season] | None = None,
) -> _MetricCatalogAvailabilityIndex:
    ordered_teams = teams or [
        Team.from_id(team_id)
        for team_id in sorted({team_season.team.team_id for team_season in cached_team_seasons})
    ]
    ordered_seasons = (
        seasons
        if seasons is not None
        else require_normalized_seasons(
            [team_season.season for team_season in cached_team_seasons]
        )
    )
    filtered_cached_team_seasons = _filter_cached_team_seasons(
        cached_team_seasons,
        team_ids={team.team_id for team in ordered_teams},
        seasons=set(ordered_seasons),
    )

    available_team_ids = [team.team_id for team in ordered_teams]
    available_season_ids = [season.year_string_nba_api for season in ordered_seasons]
    season_ids_by_team_set: dict[int, set[str]] = {}
    team_ids_by_season_set: dict[str, set[int]] = {}
    for team_season in filtered_cached_team_seasons:
        season_ids_by_team_set.setdefault(team_season.team.team_id, set()).add(
            team_season.season.year_string_nba_api
        )
        team_ids_by_season_set.setdefault(team_season.season.year_string_nba_api, set()).add(
            team_season.team.team_id
        )

    return _MetricCatalogAvailabilityIndex(
        season_type=season_type,
        teams=ordered_teams,
        seasons=ordered_seasons,
        season_ids_by_team_id={
            team_id: [
                season_id
                for season_id in available_season_ids
                if season_id in season_ids_by_team_set.get(team_id, set())
            ]
            for team_id in available_team_ids
        },
        team_ids_by_season_id={
            season_id: [
                team_id
                for team_id in available_team_ids
                if team_id in team_ids_by_season_set.get(season_id, set())
            ]
            for season_id in available_season_ids
        },
    )


def _filter_cached_team_seasons(
    cached_team_seasons: list[TeamSeasonScope],
    *,
    team_ids: set[int] | None = None,
    seasons: set[Season] | None = None,
) -> list[TeamSeasonScope]:
    return [
        team_season
        for team_season in cached_team_seasons
        if (team_ids is None or team_season.team.team_id in team_ids)
        and (seasons is None or team_season.season in seasons)
    ]


def _build_team_options(
    availability_index: _MetricCatalogAvailabilityIndex,
) -> list[dict[str, Any]]:
    return [
        {
            "team_id": team.team_id,
            "label": team.current.abbreviation,
            "available_seasons": availability_index.season_ids_by_team_id.get(team.team_id, []),
        }
        for team in sorted(availability_index.teams, key=lambda item: item.current.abbreviation)
    ]


def _build_available_teams_by_season(
    availability_index: _MetricCatalogAvailabilityIndex,
) -> dict[str, list[str]]:
    teams_by_id = {team.team_id: team for team in availability_index.teams}
    return {
        season_id: [
            teams_by_id[team_id].current.abbreviation
            for team_id in availability_index.team_ids_by_season_id.get(season_id, [])
        ]
        for season_id in [season.year_string_nba_api for season in availability_index.seasons]
    }


def _build_metric_season_span(
    *,
    season_type: SeasonType,
    start_season_id: str | None,
    end_season_id: str | None,
) -> MetricSeasonSpan | None:
    if start_season_id is None:
        return None
    assert end_season_id is not None, "metric store full-span seasons must be paired"
    start_season = Season.parse_id(start_season_id)
    end_season = Season.parse_id(end_season_id)
    assert start_season.season_type == season_type, "metric store span start season_type mismatch"
    assert end_season.season_type == season_type, "metric store span end season_type mismatch"
    return MetricSeasonSpan(
        start_season=start_season,
        end_season=end_season,
    )


def _parse_catalog_seasons(
    *,
    season_type: SeasonType,
    season_ids: list[str],
) -> list[Season]:
    seasons = [Season.parse_id(season_id) for season_id in season_ids]
    invalid_seasons = [season.id for season in seasons if season.season_type != season_type]
    assert not invalid_seasons, (
        "metric store catalog season_type does not match available seasons: "
        f"{invalid_seasons!r}"
    )
    return seasons
