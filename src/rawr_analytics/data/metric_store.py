from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.game_cache import list_cache_load_rows, list_cached_team_seasons
from rawr_analytics.data.game_cache.rows import NormalizedCacheLoadRow
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter
from rawr_analytics.data.player_metrics_db import (
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
    clear_metric_scope_store,
    load_metric_scope_store_state,
    replace_rawr_scope_snapshot,
    replace_wowy_scope_snapshot,
)
from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.rawr import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    list_incomplete_rawr_season_warnings,
    prepare_rawr_player_season_records,
)
from rawr_analytics.metrics.rawr import describe_metric as describe_rawr_metric
from rawr_analytics.metrics.wowy import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
    prepare_wowy_player_season_records,
)
from rawr_analytics.metrics.wowy import describe_metric as describe_wowy_metric
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, SeasonType
from rawr_analytics.shared.team import Team, normalize_teams, to_team_ids

RefreshProgressFn = Callable[[int, int, str], None]
DEFAULT_RAWR_RIDGE_ALPHA = 10.0


@dataclass(frozen=True)
class RefreshScopeResult:
    scope_key: str
    scope_label: str
    row_count: int
    status: str


@dataclass(frozen=True)
class MetricStoreRefreshPlan:
    metric_label: str
    build_version: str
    source_fingerprint: str
    warnings: list[str]
    failure_message: str | None = None
    available_teams: list[Team] | None = None
    scopes: list[MetricStoreRefreshScope] | None = None


@dataclass(frozen=True)
class MetricStoreRefreshScope:
    teams: list[Team] | None
    scope_key: str
    team_filter: str
    scope_label: str
    available_seasons: list[Season]


def prepare_metric_store_refresh(
    metric: Metric,
    *,
    season_type: SeasonType,
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
    include_team_scopes: bool = True,
) -> MetricStoreRefreshPlan:
    cache_load_rows = _list_cache_load_rows_for_season_type(season_type)
    if not cache_load_rows:
        return MetricStoreRefreshPlan(
            metric_label=_describe_metric(metric).label,
            build_version="",
            source_fingerprint="",
            warnings=[],
            failure_message=(
                "Normalized cache is empty for the requested season type. "
                "Rebuild ingest before refreshing the web metric store."
            ),
        )

    cached_team_seasons = _list_cached_team_scopes_for_season_type(season_type)
    available_teams = [Team.from_id(scope.team.team_id) for scope in cached_team_seasons]
    unique_available_teams = normalize_teams(available_teams) or []
    team_scopes: list[list[Team] | None] = [None]
    if include_team_scopes:
        team_scopes.extend([[team] for team in unique_available_teams])

    metric_info = _describe_metric(metric)
    source_fingerprint = _build_cache_load_fingerprint(cache_load_rows)
    build_version = (
        f"{metric_info.build_version}-alpha-{rawr_ridge_alpha:.4f}"
        if metric == Metric.RAWR
        else metric_info.build_version
    )
    warnings = (
        list_incomplete_rawr_season_warnings(season_type=season_type)
        if metric == Metric.RAWR
        else []
    )

    scopes = [
        _build_refresh_scope(
            teams=teams,
            season_type=season_type,
            cached_team_seasons=cached_team_seasons,
        )
        for teams in team_scopes
    ]
    return MetricStoreRefreshPlan(
        metric_label=metric_info.label,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        warnings=warnings,
        available_teams=unique_available_teams,
        scopes=scopes,
    )


def refresh_metric_store_scope(
    *,
    metric: Metric,
    metric_label: str,
    scope: MetricStoreRefreshScope,
    season_type: SeasonType,
    rawr_ridge_alpha: float,
    available_teams: list[Team],
    source_fingerprint: str,
    build_version: str,
) -> tuple[RefreshScopeResult, bool]:
    state = load_metric_scope_store_state(metric.value, scope.scope_key)
    if (
        state is not None
        and state.metadata.source_fingerprint == source_fingerprint
        and state.metadata.build_version == build_version
        and state.metadata.row_count > 0
    ):
        return (
            RefreshScopeResult(
                scope_key=scope.scope_key,
                scope_label=scope.scope_label,
                row_count=state.metadata.row_count,
                status="cached",
            ),
            False,
        )

    if metric == Metric.RAWR:
        rows = _build_rawr_cached_rows(
            scope_key=scope.scope_key,
            team_filter=scope.team_filter,
            season_type=season_type,
            teams=scope.teams,
            rawr_ridge_alpha=rawr_ridge_alpha,
        )
        if scope.teams is None and not rows:
            clear_metric_scope_store(
                metric=metric.value,
                scope_key=scope.scope_key,
            )
            return (
                RefreshScopeResult(
                    scope_key=scope.scope_key,
                    scope_label=scope.scope_label,
                    row_count=0,
                    status="empty",
                ),
                True,
            )
        replace_rawr_scope_snapshot(
            scope_key=scope.scope_key,
            label=metric_label,
            team_filter=scope.team_filter,
            season_type=season_type.to_nba_format(),
            available_season_ids=[season.id for season in scope.available_seasons],
            available_team_ids=[team.team_id for team in available_teams],
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            rows=rows,
        )
        row_count = len(rows)
    else:
        rows = _build_wowy_cached_rows(
            metric=metric,
            scope_key=scope.scope_key,
            team_filter=scope.team_filter,
            season_type=season_type,
            teams=scope.teams,
        )
        replace_wowy_scope_snapshot(
            metric_id=metric.value,
            scope_key=scope.scope_key,
            label=metric_label,
            team_filter=scope.team_filter,
            season_type=season_type.to_nba_format(),
            available_season_ids=[season.id for season in scope.available_seasons],
            available_team_ids=[team.team_id for team in available_teams],
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            rows=rows,
        )
        row_count = len(rows)
    return (
        RefreshScopeResult(
            scope_key=scope.scope_key,
            scope_label=scope.scope_label,
            row_count=row_count,
            status="built",
        ),
        False,
    )


def _build_refresh_scope(
    *,
    teams: list[Team] | None,
    season_type: SeasonType,
    cached_team_seasons: list[TeamSeasonScope],
) -> MetricStoreRefreshScope:
    normalized_teams = normalize_teams(teams)
    normalized_team_ids = to_team_ids(normalized_teams)
    team_filter = build_team_filter(normalized_teams)
    scope_key = build_scope_key(season_type=season_type, team_filter=team_filter)
    return MetricStoreRefreshScope(
        teams=normalized_teams,
        scope_key=scope_key,
        team_filter=team_filter,
        scope_label=(
            normalized_teams[0].current.abbreviation
            if normalized_teams and len(normalized_teams) == 1
            else team_filter or "all-teams"
        ),
        available_seasons=sorted(
            {
                scope.season
                for scope in cached_team_seasons
                if normalized_team_ids is None or scope.team.team_id in normalized_team_ids
            },
            key=lambda season: season.id,
        ),
    )


def _build_rawr_cached_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
    teams: list[Team] | None,
    rawr_ridge_alpha: float,
) -> list[RawrPlayerSeasonValueRow]:
    records = prepare_rawr_player_season_records(
        teams=teams,
        seasons=None,
        season_type=season_type,
        min_games=1,
        ridge_alpha=rawr_ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return [
        RawrPlayerSeasonValueRow(
            metric_id="rawr",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type.value,
            season_id=record.season.id,
            player_id=record.player_id,
            player_name=record.player_name,
            coefficient=record.coefficient,
            games=record.games,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
        )
        for record in records
    ]


def _build_wowy_cached_rows(
    *,
    metric: Metric,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
    teams: list[Team] | None,
) -> list[WowyPlayerSeasonValueRow]:
    records = prepare_wowy_player_season_records(
        teams=teams,
        seasons=None,
        season_type=season_type,
        min_games_with=0,
        min_games_without=0,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    values_by_player_season = None
    include_raw_wowy_score = False
    if metric == Metric.WOWY_SHRUNK:
        include_raw_wowy_score = True
        values_by_player_season = {
            (record.season, record.player_id): compute_wowy_shrinkage_score(
                games_with=record.games_with,
                games_without=record.games_without,
                wowy_score=record.wowy_score,
                prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
            )
            for record in records
        }
    rows: list[WowyPlayerSeasonValueRow] = []
    for record in records:
        value = (
            values_by_player_season[(record.season, record.player_id)]
            if values_by_player_season is not None
            else record.wowy_score
        )
        rows.append(
            WowyPlayerSeasonValueRow(
                metric_id=metric.value,
                scope_key=scope_key,
                team_filter=team_filter,
                season_type=season_type.value,
                season_id=record.season.id,
                player_id=record.player_id,
                player_name=record.player_name,
                value=value,
                games_with=record.games_with,
                games_without=record.games_without,
                avg_margin_with=record.avg_margin_with,
                avg_margin_without=record.avg_margin_without,
                average_minutes=record.average_minutes,
                total_minutes=record.total_minutes,
                raw_wowy_score=(record.wowy_score if include_raw_wowy_score else None),
            )
        )
    return rows

def _describe_metric(metric: Metric) -> MetricSummary:
    if metric == Metric.RAWR:
        return describe_rawr_metric()
    return describe_wowy_metric(metric)


def _list_cache_load_rows_for_season_type(season_type: SeasonType) -> list[NormalizedCacheLoadRow]:
    return [row for row in list_cache_load_rows() if row.season.season_type == season_type]


def _list_cached_team_scopes_for_season_type(season_type: SeasonType) -> list[TeamSeasonScope]:
    return [
        scope for scope in list_cached_team_seasons() if scope.season.season_type == season_type
    ]


def _build_cache_load_fingerprint(rows: list[NormalizedCacheLoadRow]) -> str:
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda item: (item.season.id, item.team.team_id)):
        digest.update(str(row.team.team_id).encode("utf-8"))
        digest.update(row.season.id.encode("utf-8"))
        digest.update(row.season.season_type.value.encode("utf-8"))
        digest.update(row.source_path.encode("utf-8"))
        digest.update(row.source_snapshot.encode("utf-8"))
        digest.update(row.source_kind.encode("utf-8"))
        digest.update(row.build_version.encode("utf-8"))
        digest.update(str(row.games_row_count).encode("utf-8"))
        digest.update(str(row.game_players_row_count).encode("utf-8"))
        digest.update(str(row.expected_games_row_count).encode("utf-8"))
        digest.update(str(row.skipped_games_row_count).encode("utf-8"))
    return digest.hexdigest()


__all__ = [
    "DEFAULT_RAWR_RIDGE_ALPHA",
    "MetricStoreRefreshPlan",
    "MetricStoreRefreshScope",
    "RefreshScopeResult",
    "prepare_metric_store_refresh",
    "refresh_metric_store_scope",
]
