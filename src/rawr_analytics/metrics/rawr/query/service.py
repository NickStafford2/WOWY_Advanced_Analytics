from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.data.metric_store.rawr import (
    RawrPlayerSeasonValueRow,
    load_rawr_player_season_value_rows,
)
from rawr_analytics.data.metric_store.store import load_metric_cache_store_state
from rawr_analytics.data.metric_store.usage import record_metric_cache_query
from rawr_analytics.metrics._metric_cache_key import build_rawr_metric_cache_key
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr.cache import load_rawr_records
from rawr_analytics.metrics.rawr.calculate.inputs import build_rawr_request_from_calc_vars
from rawr_analytics.metrics.rawr.calculate.records import (
    RawrPlayerSeasonRecord,
    build_player_season_records,
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
    season_ids,
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

    seasons = list(dict.fromkeys(query.calc_vars.seasons))
    teams_by_id = {team.team_id: team for team in query.calc_vars.teams}
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
    record_metric_cache_query(
        metric_id=Metric.RAWR.value,
        metric_cache_key=build_rawr_metric_cache_key(query.calc_vars),
    )
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
        top_n=query.post_calc_filters.top_n,
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
        top_n=query.post_calc_filters.top_n,
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
        teams=query.calc_vars.teams,
        seasons=query.calc_vars.seasons,
        progress_fn=progress_fn,
    )
    request = build_rawr_request_from_calc_vars(
        calc_vars=query.calc_vars,
        season_games=season_games,
        season_game_players=season_game_players,
        filters=query.post_calc_filters.filters,
    )
    return build_player_season_records(request)


def _try_load_rawr_store_result(query: RawrQuery) -> ResolvedRawrResultDTO | None:
    cache_key = _resolve_cached_rawr_key(query)
    if cache_key is None:
        return None

    available = _try_load_current_metric_availability(
        metric=Metric.RAWR,
        metric_cache_key=cache_key,
        query=query,
    )
    if available is None:
        return None

    rows = [
        _build_rawr_record_from_store_row(row)
        for row in load_rawr_player_season_value_rows(
            metric_cache_key=cache_key,
            seasons=season_ids(query.calc_vars.seasons),
            min_average_minutes=query.post_calc_filters.filters.min_average_minutes,
            min_total_minutes=query.post_calc_filters.filters.min_total_minutes,
            min_games=query.calc_vars.eligibility.min_games,
        )
    ]
    return ResolvedRawrResultDTO(
        rows=rows,
        seasons=_selected_rawr_seasons(query, rows),
        source="cache",
        available_teams=available.available_teams,
        available_seasons=available.available_seasons,
    )

def _selected_rawr_seasons(query: RawrQuery, rows: list[RawrPlayerSeasonRecord]) -> list[Season]:
    selected_seasons = normalize_seasons([row.season for row in rows]) or []
    if selected_seasons:
        return selected_seasons
    requested_seasons = query.calc_vars.seasons or build_all_nba_history_seasons()
    return normalize_seasons(requested_seasons) or []


def _unique_season_years(seasons: list[Season]) -> list[str]:
    return list(dict.fromkeys(season.year_string_nba_api for season in seasons))


def _build_rawr_record_from_store_row(row: RawrPlayerSeasonValueRow) -> RawrPlayerSeasonRecord:
    return RawrPlayerSeasonRecord(
        season=Season.parse_id(row.season_id),
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


def _resolve_cached_rawr_key(query: RawrQuery) -> str | None:
    cache_snapshot = load_game_cache_snapshot(
        teams=query.calc_vars.teams,
        seasons=query.calc_vars.seasons,
    )
    if not cache_snapshot.entries:
        return None

    return build_rawr_metric_cache_key(query.calc_vars)


@dataclass(frozen=True)
class _CachedRawrAvailability:
    available_teams: list[Team]
    available_seasons: list[Season]


def _try_load_current_metric_availability(
    *,
    metric: Metric,
    metric_cache_key: str,
    query: RawrQuery,
) -> _CachedRawrAvailability | None:
    state = load_metric_cache_store_state(metric.value, metric_cache_key)
    if state is None:
        return None

    cache_snapshot = load_game_cache_snapshot(
        teams=query.calc_vars.teams,
        seasons=query.calc_vars.seasons,
    )
    if not cache_snapshot.entries:
        return None

    if state.cache_entry_state.source_fingerprint != cache_snapshot.fingerprint:
        return None

    seasons = list(dict.fromkeys(query.calc_vars.seasons or build_all_nba_history_seasons()))
    teams_by_id = {team.team_id: team for team in query.calc_vars.teams}
    teams = list(teams_by_id.values())

    seasons = [season for season in seasons if any(team.is_active_during(season) for team in teams)]
    teams = [team for team in teams if any(team.is_active_during(season) for season in seasons)]

    return _CachedRawrAvailability(
        available_teams=teams,
        available_seasons=seasons,
    )
