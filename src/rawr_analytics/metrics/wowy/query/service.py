from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from rawr_analytics.data.game_cache.store import load_game_cache_snapshot
from rawr_analytics.data.metric_store.store import load_metric_cache_store_state
from rawr_analytics.data.metric_store.wowy import (
    WowyPlayerSeasonValueRow,
    load_wowy_player_season_value_rows,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics._metric_cache_key import build_wowy_metric_cache_key
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
from rawr_analytics.metrics.wowy.query.request import WowyQuery
from rawr_analytics.shared.common import JSONDict
from rawr_analytics.shared.player import PlayerMinutes, PlayerSummary
from rawr_analytics.shared.season import (
    Season,
    build_all_nba_history_seasons,
    normalize_seasons,
    season_ids,
)
from rawr_analytics.shared.team import Team, build_metric_team_filter

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
    games, game_players = load_wowy_records(
        teams=query.calc_vars.teams,
        seasons=query.calc_vars.seasons,
    )
    season_inputs = build_wowy_season_inputs(games=games, game_players=game_players)
    return build_wowy_custom_query(
        metric,
        season_inputs=season_inputs,
        eligibility=query.calc_vars.eligibility,
        filters=query.post_calc_filters.filters,
    )


def _try_load_wowy_store_result(
    *,
    metric: Metric,
    query: WowyQuery,
) -> ResolvedWowyResultDTO | None:
    cache_key = _resolve_cached_wowy_key(metric=metric, query=query)
    if cache_key is None:
        return None

    available = _try_load_current_metric_availability(
        metric=metric,
        query=query,
        metric_cache_key=cache_key,
    )
    if available is None:
        return None

    rows = [
        _build_wowy_value_from_store_row(row)
        for row in load_wowy_player_season_value_rows(
            metric_id=metric.value,
            metric_cache_key=cache_key,
            seasons=season_ids(query.calc_vars.seasons),
            min_average_minutes=query.post_calc_filters.filters.min_average_minutes,
            min_total_minutes=query.post_calc_filters.filters.min_total_minutes,
            min_games_with=query.calc_vars.eligibility.min_games_with,
            min_games_without=query.calc_vars.eligibility.min_games_without,
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


def _resolve_cached_wowy_key(
    *,
    metric: Metric,
    query: WowyQuery,
) -> str | None:
    team_filter = build_metric_team_filter(query.calc_vars.teams)
    if team_filter:
        return None

    cache_snapshot = load_game_cache_snapshot()
    if not cache_snapshot.entries:
        return None

    return build_wowy_metric_cache_key(metric_id=metric.value, calc_vars=query.calc_vars)


@dataclass(frozen=True)
class _CachedWowyAvailability:
    available_teams: list[Team]
    available_seasons: list[Season]


def _try_load_current_metric_availability(
    *,
    metric: Metric,
    query: WowyQuery,
    metric_cache_key: str,
) -> _CachedWowyAvailability | None:
    state = load_metric_cache_store_state(metric.value, metric_cache_key)
    if state is None:
        return None

    cache_snapshot = load_game_cache_snapshot()
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
