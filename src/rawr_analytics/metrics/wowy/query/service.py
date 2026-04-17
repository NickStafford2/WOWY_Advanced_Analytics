from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.data.metric_store.store import load_metric_cache_store_state
from rawr_analytics.data.metric_store.usage import record_metric_cache_query
from rawr_analytics.data.metric_store.wowy import (
    WowyPlayerSeasonValueRow,
    load_wowy_player_season_value_rows,
    replace_wowy_metric_cache,
)
from rawr_analytics.metrics._player_context import PlayerSeasonFilters
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics._metric_cache_key import build_wowy_metric_cache_key
from rawr_analytics.metrics.wowy._calc_vars import WowyCalcVars
from rawr_analytics.metrics.wowy.cache import load_wowy_records
from rawr_analytics.metrics.wowy.calculate.inputs import build_wowy_season_inputs
from rawr_analytics.metrics.wowy.calculate.records import (
    WowyPlayerSeasonValue,
    build_wowy_custom_query,
    build_wowy_player_season_value,
)
from rawr_analytics.metrics.wowy.query.presenters import WowyQueryFiltersDTO
from rawr_analytics.metrics.wowy.query.presenters import (
    build_wowy_leaderboard_payload as build_wowy_leaderboard_payload_from_values,
)
from rawr_analytics.metrics.wowy.query.presenters import (
    build_wowy_player_seasons_payload as build_wowy_player_seasons_payload_from_values,
)
from rawr_analytics.metrics.wowy.query.presenters import (
    build_wowy_span_chart_payload as build_wowy_span_chart_payload_from_values,
)
from rawr_analytics.metrics.wowy.query.request import WowyPostCalcFilters, WowyQuery
from rawr_analytics.shared.common import JSONDict
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import (
    Season,
    build_all_nba_history_seasons,
    normalize_seasons,
    season_ids,
)
from rawr_analytics.shared.team import Team

type WowyResultSource = Literal["cache", "live"]
type MetricQueryExport = list[JSONDict]


@dataclass(frozen=True)
class ResolvedWowyResultDTO:
    player_season_value: list[WowyPlayerSeasonValue]
    seasons: list[Season]
    source: WowyResultSource
    available_teams: list[Team] | None
    available_seasons: list[Season] | None
    metric: Metric


@dataclass(frozen=True)
class EnsureWowyMetricCacheResult:
    metric_cache_key: str
    row_count: int
    status: str


def build_wowy_options_payload(
    query: WowyQuery,
    *,
    metric: Metric = Metric.WOWY,
) -> JSONDict:
    _require_wowy_metric(metric)
    filters = WowyQueryFiltersDTO.from_query(query).for_options()

    seasons = list(dict.fromkeys(query.calc_vars.seasons or build_all_nba_history_seasons()))
    teams_by_id = {team.team_id: team for team in query.calc_vars.teams}
    teams = list(teams_by_id.values())

    seasons = [season for season in seasons if any(team.is_active_during(season) for team in teams)]
    teams = [team for team in teams if any(team.is_active_during(season) for season in seasons)]

    return _build_static_metric_options_payload(
        metric=metric,
        seasons=seasons,
        teams=teams,
        filters=filters.to_payload(),
    )


def _build_static_metric_options_payload(
    *,
    metric: Metric,
    seasons: list[Season],
    teams: list[Team],
    filters: JSONDict,
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


def resolve_wowy_result(
    query: WowyQuery,
    *,
    metric: Metric = Metric.WOWY,
    recalculate: bool = False,
) -> ResolvedWowyResultDTO:
    _require_wowy_metric(metric)
    record_metric_cache_query(
        metric_id=metric.value,
        metric_cache_key=build_wowy_metric_cache_key(
            metric_id=metric.value,
            calc_vars=query.calc_vars,
        ),
    )

    if not recalculate:
        cached_result = _try_load_wowy_store_result(metric=metric, query=query)
        if cached_result is not None:
            return cached_result

    live_rows = _build_live_wowy_query_result(metric=metric, query=query)
    seasons = _selected_wowy_seasons(query, live_rows)
    return ResolvedWowyResultDTO(
        player_season_value=live_rows,
        seasons=seasons,
        source="live",
        available_teams=None,
        available_seasons=None,
        metric=metric,
    )


def build_wowy_leaderboard_payload(
    query: WowyQuery,
    result: ResolvedWowyResultDTO,
) -> JSONDict:
    payload = build_wowy_leaderboard_payload_from_values(
        metric=result.metric,
        rows=result.player_season_value,
        seasons=result.seasons,
        top_n=query.post_calc_filters.top_n,
        mode=result.source,
        available_seasons=result.available_seasons,
        available_teams=result.available_teams,
    )
    payload["filters"] = WowyQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_wowy_player_seasons_payload(
    query: WowyQuery,
    result: ResolvedWowyResultDTO,
) -> JSONDict:
    payload = build_wowy_player_seasons_payload_from_values(
        metric=result.metric,
        rows=result.player_season_value,
    )
    payload["filters"] = WowyQueryFiltersDTO.from_query(query).to_payload()
    return payload


def build_wowy_span_chart_payload(
    query: WowyQuery,
    result: ResolvedWowyResultDTO,
) -> JSONDict:
    payload = build_wowy_span_chart_payload_from_values(
        metric=result.metric,
        rows=result.player_season_value,
        seasons=result.seasons,
        top_n=query.post_calc_filters.top_n,
    )
    payload["filters"] = WowyQueryFiltersDTO.from_query(query).to_payload()
    return payload


def _build_live_wowy_query_result(
    *,
    metric: Metric,
    query: WowyQuery,
) -> list[WowyPlayerSeasonValue]:
    calc_vars = query.calc_vars
    games, game_players = load_wowy_records(
        teams=calc_vars.teams,
        seasons=calc_vars.seasons,
    )
    season_inputs = build_wowy_season_inputs(games=games, game_players=game_players)
    return build_wowy_custom_query(
        metric,
        calc_vars=calc_vars,
        season_inputs=season_inputs,
        filters=query.post_calc_filters.filters,
    )


def ensure_wowy_metric_cache(
    *,
    metric: Metric,
    calc_vars: WowyCalcVars,
    build_version: str,
) -> EnsureWowyMetricCacheResult:
    _require_wowy_metric(metric)
    query = WowyQuery(
        calc_vars=calc_vars,
        post_calc_filters=WowyPostCalcFilters(
            top_n=0,
            filters=PlayerSeasonFilters(
                min_average_minutes=None,
                min_total_minutes=None,
            ),
        ),
    )
    cache_key = build_wowy_metric_cache_key(
        metric_id=metric.value,
        calc_vars=query.calc_vars,
    )
    cache_snapshot = load_game_cache_snapshot(
        teams=query.calc_vars.teams,
        seasons=query.calc_vars.seasons,
    )
    if not cache_snapshot.entries:
        raise ValueError("Cannot build WOWY metric cache without cached source games")

    state = load_metric_cache_store_state(metric.value, cache_key)
    if (
        state is not None
        and state.cache_entry_state.source_fingerprint == cache_snapshot.fingerprint
        and state.cache_entry_state.build_version == build_version
        and state.cache_entry_state.row_count > 0
    ):
        return EnsureWowyMetricCacheResult(
            metric_cache_key=cache_key,
            row_count=state.cache_entry_state.row_count,
            status="cached",
        )

    live_rows = _build_live_wowy_query_result(metric=metric, query=query)
    store_rows = [
        _build_wowy_store_row_from_value(metric=metric, row=row) for row in live_rows
    ]
    replace_wowy_metric_cache(
        metric_id=metric.value,
        metric_cache_key=cache_key,
        seasons=query.calc_vars.seasons,
        build_version=build_version,
        source_fingerprint=cache_snapshot.fingerprint,
        rows=store_rows,
    )
    return EnsureWowyMetricCacheResult(
        metric_cache_key=cache_key,
        row_count=len(store_rows),
        status="built",
    )


def _try_load_wowy_store_result(
    *,
    metric: Metric,
    query: WowyQuery,
) -> ResolvedWowyResultDTO | None:
    calc_vars = query.calc_vars
    cache_key = _resolve_cached_wowy_key(metric=metric, calc_vars=calc_vars)
    if cache_key is None:
        return None

    available = _try_load_current_metric_availability(
        metric=metric,
        query=query,
        calc_vars=calc_vars,
        metric_cache_key=cache_key,
    )
    if available is None:
        return None

    rows = [
        _build_wowy_value_from_store_row(row)
        for row in load_wowy_player_season_value_rows(
            metric_id=metric.value,
            metric_cache_key=cache_key,
            seasons=season_ids(calc_vars.seasons),
            min_average_minutes=query.post_calc_filters.filters.min_average_minutes,
            min_total_minutes=query.post_calc_filters.filters.min_total_minutes,
            min_games_with=calc_vars.eligibility.min_games_with,
            min_games_without=calc_vars.eligibility.min_games_without,
        )
    ]

    return ResolvedWowyResultDTO(
        player_season_value=rows,
        seasons=_selected_wowy_seasons(query, rows),
        source="cache",
        available_teams=available.available_teams,
        available_seasons=available.available_seasons,
        metric=metric,
    )

def _selected_wowy_seasons(
    query: WowyQuery,
    rows: list[WowyPlayerSeasonValue],
) -> list[Season]:
    selected_seasons = normalize_seasons([row.season for row in rows]) or []
    if selected_seasons:
        return selected_seasons

    requested_seasons = query.calc_vars.seasons or build_all_nba_history_seasons()
    return normalize_seasons(requested_seasons) or []


def _require_wowy_metric(metric: Metric) -> None:
    if metric not in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        raise ValueError(f"Unknown WOWY metric: {metric}")


def _unique_season_years(seasons: list[Season]) -> list[str]:
    return list(dict.fromkeys(season.year_string_nba_api for season in seasons))


def _build_wowy_value_from_store_row(
    row: WowyPlayerSeasonValueRow,
) -> WowyPlayerSeasonValue:
    season = Season.parse_id(row.season_id)
    return build_wowy_player_season_value(
        season=season,
        player=PlayerSummary(
            player_id=row.player_id,
            player_name=row.player_name,
        ),
        minutes=PlayerMinutes(
            average_minutes=row.average_minutes,
            total_minutes=row.total_minutes,
        ),
        games_with=row.games_with,
        games_without=row.games_without,
        avg_margin_with=row.avg_margin_with,
        avg_margin_without=row.avg_margin_without,
        value=row.value,
        raw_value=row.raw_wowy_score,
    )


def _build_wowy_store_row_from_value(
    *,
    metric: Metric,
    row: WowyPlayerSeasonValue,
) -> WowyPlayerSeasonValueRow:
    return WowyPlayerSeasonValueRow(
        season_id=row.season.id,
        player_id=row.player.player_id,
        player_name=row.player.player_name,
        value=row.result.value,
        games_with=row.result.games_with,
        games_without=row.result.games_without,
        avg_margin_with=row.result.avg_margin_with,
        avg_margin_without=row.result.avg_margin_without,
        average_minutes=row.minutes.average_minutes,
        total_minutes=row.minutes.total_minutes,
        raw_wowy_score=row.result.raw_value if metric == Metric.WOWY_SHRUNK else None,
    )


def _resolve_cached_wowy_key(
    *,
    metric: Metric,
    calc_vars: WowyCalcVars,
) -> str | None:
    cache_snapshot = load_game_cache_snapshot(
        teams=calc_vars.teams,
        seasons=calc_vars.seasons,
    )
    if not cache_snapshot.entries:
        return None

    return build_wowy_metric_cache_key(metric_id=metric.value, calc_vars=calc_vars)


@dataclass(frozen=True)
class _CachedWowyAvailability:
    available_teams: list[Team]
    available_seasons: list[Season]


def _try_load_current_metric_availability(
    *,
    metric: Metric,
    query: WowyQuery,
    calc_vars: WowyCalcVars,
    metric_cache_key: str,
) -> _CachedWowyAvailability | None:
    state = load_metric_cache_store_state(metric.value, metric_cache_key)
    if state is None:
        return None

    cache_snapshot = load_game_cache_snapshot(
        teams=calc_vars.teams,
        seasons=calc_vars.seasons,
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

    return _CachedWowyAvailability(
        available_teams=teams,
        available_seasons=seasons,
    )
