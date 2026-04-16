from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.data.metric_store._reads import load_metric_scope_store_state
from rawr_analytics.data.metric_store.rawr import (
    RawrPlayerSeasonValueRow,
    load_rawr_player_season_value_rows,
)
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter, season_ids
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr.cache import load_rawr_records
from rawr_analytics.metrics.rawr.calculate.inputs import build_rawr_request
from rawr_analytics.metrics.rawr.calculate.records import (
    RawrPlayerSeasonRecord,
    build_player_season_records,
)
from rawr_analytics.metrics.rawr.defaults import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
)
from rawr_analytics.metrics.rawr.query.presenters import RawrQueryFiltersDTO
from rawr_analytics.metrics.rawr.query.presenters import (
    build_rawr_leaderboard_payload as build_rawr_leaderboard_payload_from_records,
)
from rawr_analytics.metrics.rawr.query.presenters import (
    build_rawr_player_seasons_payload as build_rawr_player_seasons_payload_from_records,
)
from rawr_analytics.metrics.rawr.query.presenters import (
    build_rawr_span_chart_payload as build_rawr_span_chart_payload_from_records,
)
from rawr_analytics.metrics.rawr.query.request import RawrQuery
from rawr_analytics.shared.common import JSONDict
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import (
    Season,
    build_all_nba_history_seasons,
    normalize_seasons,
)
from rawr_analytics.shared.team import Team

type RawrProgressFn = Callable[[int, int, Season], None]
type MetricQueryExport = list[JSONDict]
type RawrResultSource = Literal["cache", "live"]


@dataclass(frozen=True)
class ResolvedRawrResultDTO:
    rows: list[RawrPlayerSeasonRecord]
    seasons: list[Season]
    source: RawrResultSource
    available_teams: list[Team] | None
    available_seasons: list[Season] | None


def build_rawr_options_payload(query: RawrQuery) -> JSONDict:
    filters = RawrQueryFiltersDTO.from_query(query).for_options()

    seasons = list(dict.fromkeys(query.seasons))
    teams_by_id = {team.team_id: team for team in query.teams}
    teams = list(teams_by_id.values())

    seasons = [season for season in seasons if any(team.is_active_during(season) for team in teams)]
    teams = [team for team in teams if any(team.is_active_during(season) for season in seasons)]

    return _build_static_metric_options_payload(
        metric=Metric.RAWR,
        seasons=seasons,
        teams=teams,
        filters=filters.to_payload(),
    )


def _build_static_metric_options_payload(
    *,
    metric: Metric,
    seasons: list[Season],
    teams: list[Team],
    filters: dict[str, Any],
) -> JSONDict:
    ordered_teams = sorted(teams, key=lambda team: team.current.abbreviation)
    season_ids = _unique_season_years(seasons)

    return {
        "metric": metric.value,
        "available_teams": [team.current.abbreviation for team in ordered_teams],
        "team_options": [
            {
                "team_id": team.team_id,
                "label": team.current.abbreviation,
                "available_seasons": _unique_season_years(
                    [season for season in seasons if team.is_active_during(season)]
                ),
            }
            for team in ordered_teams
        ],
        "available_seasons": season_ids,
        "available_teams_by_season": {
            season.year_string_nba_api: [
                team.abbreviation(season=season)
                for team in ordered_teams
                if team.is_active_during(season)
            ]
            for season in seasons
        },
        "filters": filters,
    }


def resolve_rawr_result(
    query: RawrQuery,
    *,
    recalculate: bool = False,
    progress_fn: RawrProgressFn | None = None,
) -> ResolvedRawrResultDTO:
    if not recalculate:
        cached_result = _try_load_rawr_store_result(query)
        if cached_result is not None:
            return cached_result

    live_rows = _build_live_rawr_query_result(query, progress_fn=progress_fn)
    return ResolvedRawrResultDTO(
        rows=live_rows,
        seasons=_selected_rawr_seasons(query, live_rows),
        source="live",
        available_teams=None,
        available_seasons=None,
    )


def build_rawr_leaderboard_payload(
    query: RawrQuery,
    result: ResolvedRawrResultDTO,
    *,
    recalculate: bool = False,
) -> JSONDict:
    payload = build_rawr_leaderboard_payload_from_records(
        metric=Metric.RAWR.value,
        rows=result.rows,
        seasons=result.seasons,
        top_n=query.top_n,
        mode=result.source,
        available_seasons=result.available_seasons,
        available_teams=result.available_teams,
    )
    payload["filters"] = RawrQueryFiltersDTO.from_query(
        query,
        recalculate=recalculate,
    ).to_payload()
    return payload


def build_rawr_player_seasons_payload(
    query: RawrQuery,
    result: ResolvedRawrResultDTO,
    *,
    recalculate: bool = False,
) -> JSONDict:
    payload = build_rawr_player_seasons_payload_from_records(result.rows)
    payload["filters"] = RawrQueryFiltersDTO.from_query(
        query,
        recalculate=recalculate,
    ).to_payload()
    return payload


def build_rawr_span_chart_payload(
    query: RawrQuery,
    result: ResolvedRawrResultDTO,
    *,
    recalculate: bool = False,
) -> JSONDict:
    payload = build_rawr_span_chart_payload_from_records(
        metric=Metric.RAWR.value,
        rows=result.rows,
        seasons=result.seasons,
        top_n=query.top_n,
    )
    payload["filters"] = RawrQueryFiltersDTO.from_query(
        query,
        recalculate=recalculate,
    ).to_payload()
    return payload


def _build_live_rawr_query_result(
    query: RawrQuery,
    *,
    progress_fn: RawrProgressFn | None = None,
) -> list[RawrPlayerSeasonRecord]:
    season_games, season_game_players = load_rawr_records(
        teams=query.teams,
        seasons=query.seasons,
        progress_fn=progress_fn,
    )
    request = build_rawr_request(
        season_games=season_games,
        season_game_players=season_game_players,
        eligibility=query.eligibility,
        filters=query.filters,
        ridge_alpha=query.ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    )
    return build_player_season_records(request)


def _try_load_rawr_store_result(query: RawrQuery) -> ResolvedRawrResultDTO | None:
    scope_key = _resolve_all_teams_snapshot_scope_key(query)
    if scope_key is None:
        return None

    available = _try_load_current_metric_availability(
        metric=Metric.RAWR,
        scope_key=scope_key,
        query=query,
    )
    if available is None:
        return None

    season_type = query.seasons[0].season_type.value
    rows = [
        _build_rawr_record_from_store_row(row, season_type)
        for row in load_rawr_player_season_value_rows(
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.filters.min_average_minutes,
            min_total_minutes=query.filters.min_total_minutes,
            min_games=query.eligibility.min_games,
        )
    ]
    return ResolvedRawrResultDTO(
        rows=rows,
        seasons=_selected_rawr_seasons(query, rows),
        source="cache",
        available_teams=available.available_teams,
        available_seasons=available.available_seasons,
    )


# do i need this at all? i think this should be removed. I don't know if this behavior is desirable
def _selected_rawr_seasons(query: RawrQuery, rows: list[RawrPlayerSeasonRecord]) -> list[Season]:
    selected_seasons = normalize_seasons([row.season for row in rows]) or []
    if selected_seasons:
        return selected_seasons
    requested_seasons = query.seasons or build_all_nba_history_seasons()
    return normalize_seasons(requested_seasons) or []


def _unique_season_years(seasons: list[Season]) -> list[str]:
    return list(dict.fromkeys(season.year_string_nba_api for season in seasons))


def _build_rawr_record_from_store_row(
    row: RawrPlayerSeasonValueRow,
    season_type: str,
) -> RawrPlayerSeasonRecord:
    return RawrPlayerSeasonRecord(
        season=Season.parse(row.season_id, season_type),
        player=PlayerSummary(
            player_id=row.player_id,
            player_name=row.player_name,
        ),
        minutes=PlayerMinutes(
            average_minutes=row.average_minutes,
            total_minutes=row.total_minutes,
        ),
        games=row.games,
        coefficient=row.coefficient,
    )


def _resolve_all_teams_snapshot_scope_key(query: RawrQuery) -> str | None:
    team_filter = build_team_filter(query.teams)
    if team_filter:
        return None

    cache_snapshot = load_game_cache_snapshot()
    if not cache_snapshot.entries:
        return None

    seasons = query.seasons or build_all_nba_history_seasons()
    return build_scope_key(
        seasons=seasons,
        team_filter=team_filter,
    )


@dataclass(frozen=True)
class _CachedRawrAvailability:
    available_teams: list[Team]
    available_seasons: list[Season]


def _try_load_current_metric_availability(
    *,
    metric: Metric,
    scope_key: str,
    query: RawrQuery,
) -> _CachedRawrAvailability | None:
    state = load_metric_scope_store_state(metric.value, scope_key)
    if state is None:
        return None

    cache_snapshot = load_game_cache_snapshot()
    if not cache_snapshot.entries:
        return None

    if state.snapshot_state.source_fingerprint != cache_snapshot.fingerprint:
        return None

    seasons = list(dict.fromkeys(query.seasons or build_all_nba_history_seasons()))
    teams_by_id = {team.team_id: team for team in query.teams}
    teams = list(teams_by_id.values())

    seasons = [season for season in seasons if any(team.is_active_during(season) for team in teams)]
    teams = [team for team in teams if any(team.is_active_during(season) for season in seasons)]

    return _CachedRawrAvailability(
        available_teams=teams,
        available_seasons=seasons,
    )
