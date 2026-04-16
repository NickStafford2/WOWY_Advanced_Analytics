from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from rawr_analytics.data.game_cache.store import load_cache_snapshot
from rawr_analytics.data.metric_store._reads import load_metric_scope_store_state
from rawr_analytics.data.metric_store.wowy import (
    WowyPlayerSeasonValueRow,
    load_wowy_player_season_value_rows,
)
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter, season_ids
from rawr_analytics.metrics.constants import Metric
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
from rawr_analytics.shared.season import Season, build_all_nba_history_seasons, normalize_seasons
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


def build_wowy_options_payload(
    query: WowyQuery,
    *,
    metric: Metric = Metric.WOWY,
) -> JSONDict:
    _require_wowy_metric(metric)
    filters = WowyQueryFiltersDTO.from_query(query).for_options()

    seasons = list(dict.fromkeys(query.seasons or build_all_nba_history_seasons()))
    teams_by_id = {team.team_id: team for team in query.teams}
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
        top_n=query.top_n,
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
        top_n=query.top_n,
    )
    payload["filters"] = WowyQueryFiltersDTO.from_query(query).to_payload()
    return payload


def _build_live_wowy_query_result(
    *,
    metric: Metric,
    query: WowyQuery,
) -> list[WowyPlayerSeasonValue]:
    games, game_players = load_wowy_records(
        teams=query.teams,
        seasons=query.seasons,
    )
    season_inputs = build_wowy_season_inputs(games=games, game_players=game_players)
    return build_wowy_custom_query(
        metric,
        season_inputs=season_inputs,
        eligibility=query.eligibility,
        filters=query.filters,
    )


def _try_load_wowy_store_result(
    *,
    metric: Metric,
    query: WowyQuery,
) -> ResolvedWowyResultDTO | None:
    scope_key = _resolve_all_teams_snapshot_scope_key(query)
    if scope_key is None:
        return None

    available = _try_load_current_metric_availability(
        metric=metric,
        query=query,
        scope_key=scope_key,
    )
    if available is None:
        return None

    season_type = query.seasons[0].season_type.value
    rows = [
        _build_wowy_value_from_store_row(row, season_type)
        for row in load_wowy_player_season_value_rows(
            metric_id=metric.value,
            scope_key=scope_key,
            seasons=season_ids(query.seasons),
            min_average_minutes=query.filters.min_average_minutes,
            min_total_minutes=query.filters.min_total_minutes,
            min_games_with=query.eligibility.min_games_with,
            min_games_without=query.eligibility.min_games_without,
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

    requested_seasons = query.seasons or build_all_nba_history_seasons()
    return normalize_seasons(requested_seasons) or []


def _require_wowy_metric(metric: Metric) -> None:
    if metric not in {Metric.WOWY, Metric.WOWY_SHRUNK}:
        raise ValueError(f"Unknown WOWY metric: {metric}")


def _unique_season_years(seasons: list[Season]) -> list[str]:
    return list(dict.fromkeys(season.year_string_nba_api for season in seasons))


def _build_wowy_value_from_store_row(
    row: WowyPlayerSeasonValueRow,
    season_type: str,
) -> WowyPlayerSeasonValue:
    season = Season.parse(row.season_id, season_type)
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


def _resolve_all_teams_snapshot_scope_key(query: WowyQuery) -> str | None:
    team_filter = build_team_filter(query.teams)
    if team_filter:
        return None

    cache_snapshot = load_cache_snapshot()
    if not cache_snapshot.entries:
        return None

    seasons = query.seasons or build_all_nba_history_seasons()
    return build_scope_key(
        seasons=seasons,
        team_filter=team_filter,
    )


@dataclass(frozen=True)
class _CachedWowyAvailability:
    available_teams: list[Team]
    available_seasons: list[Season]


def _try_load_current_metric_availability(
    *,
    metric: Metric,
    query: WowyQuery,
    scope_key: str,
) -> _CachedWowyAvailability | None:
    state = load_metric_scope_store_state(metric.value, scope_key)
    if state is None:
        return None

    cache_snapshot = load_cache_snapshot()
    if not cache_snapshot.entries:
        return None

    if state.snapshot_state.source_fingerprint != cache_snapshot.fingerprint:
        return None

    seasons = list(dict.fromkeys(query.seasons or build_all_nba_history_seasons()))
    teams_by_id = {team.team_id: team for team in query.teams}
    teams = list(teams_by_id.values())

    seasons = [season for season in seasons if any(team.is_active_during(season) for team in teams)]
    teams = [team for team in teams if any(team.is_active_during(season) for season in seasons)]

    return _CachedWowyAvailability(
        available_teams=teams,
        available_seasons=seasons,
    )
