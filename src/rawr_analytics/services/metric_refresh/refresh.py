from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.game_cache import (
    NormalizedCacheLoadRow,
    build_normalized_cache_fingerprint,
    list_cache_load_rows,
    list_cached_team_seasons,
)
from rawr_analytics.data.metric_store import (
    MetricScopeAvailability,
    MetricScopeCatalog,
    MetricSeasonSpanIds,
    RawrPlayerSeasonValueRow,
    WowyPlayerSeasonValueRow,
    clear_metric_scope_store,
    load_metric_scope_store_state,
    replace_rawr_scope_snapshot,
    replace_wowy_scope_snapshot,
)
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter
from rawr_analytics.metrics.constants import Metric, MetricSummary
from rawr_analytics.metrics.rawr import (
    DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    DEFAULT_RAWR_SHRINKAGE_MODE,
    DEFAULT_RAWR_SHRINKAGE_STRENGTH,
    RAWR_METRIC_SUMMARY,
)
from rawr_analytics.metrics.rawr.records import build_player_season_records
from rawr_analytics.metrics.wowy import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
    prepare_wowy_player_season_records,
)
from rawr_analytics.metrics.wowy import describe_metric as describe_wowy_metric
from rawr_analytics.services._metric_inputs import (
    list_incomplete_rawr_season_warnings,
    load_rawr_request,
    load_wowy_season_inputs,
)
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import SeasonType
from rawr_analytics.shared.team import Team, normalize_teams, to_team_ids

MetricStoreRefreshEventFn = Callable[["MetricStoreRefreshProgressEvent"], None]
DEFAULT_RAWR_RIDGE_ALPHA = 10.0


@dataclass(frozen=True)
class RefreshScopeResult:
    scope_key: str
    scope_label: str
    row_count: int
    status: str


@dataclass(frozen=True)
class MetricStoreRefreshProgressEvent:
    current: int
    total: int
    detail: str


@dataclass(frozen=True)
class RefreshMetricStoreResult:
    metric: Metric
    scope_results: list[RefreshScopeResult]
    warnings: list[str]
    failure_message: str | None = None

    @property
    def ok(self) -> bool:
        return self.failure_message is None

    @property
    def total_rows(self) -> int:
        return sum(scope.row_count for scope in self.scope_results)


@dataclass(frozen=True)
class _MetricStoreRefreshPlan:
    build_version: str
    source_fingerprint: str
    warnings: list[str]
    failure_message: str | None = None
    available_teams: list[Team] | None = None
    scopes: list[_MetricStoreRefreshScope] | None = None


@dataclass(frozen=True)
class _MetricStoreRefreshScope:
    teams: list[Team] | None
    scope_key: str
    scope_label: str
    catalog: MetricScopeCatalog


def refresh_metric_store(
    *,
    metric: Metric | str,
    season_type: SeasonType | str,
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
    include_team_scopes: bool = True,
    event_fn: MetricStoreRefreshEventFn | None = None,
) -> RefreshMetricStoreResult:
    normalized_metric = Metric.parse(metric) if isinstance(metric, str) else metric
    normalized_season_type = (
        SeasonType.parse(season_type) if isinstance(season_type, str) else season_type
    )
    plan = _prepare_metric_store_refresh(
        normalized_metric,
        season_type=normalized_season_type,
        rawr_ridge_alpha=rawr_ridge_alpha,
        include_team_scopes=include_team_scopes,
    )
    if plan.failure_message is not None:
        return RefreshMetricStoreResult(
            metric=normalized_metric,
            scope_results=[],
            warnings=plan.warnings,
            failure_message=plan.failure_message,
        )

    scope_results: list[RefreshScopeResult] = []
    scopes = plan.scopes or []
    available_teams = plan.available_teams or []
    failure_message: str | None = None
    for index, scope in enumerate(scopes):
        _emit_progress(
            event_fn,
            current=index,
            total=len(scopes),
            detail=f"building {scope.scope_label}",
        )

        scope_result, should_fail_empty_rawr_scope = _refresh_metric_store_scope(
            metric=normalized_metric,
            scope=scope,
            season_type=normalized_season_type,
            rawr_ridge_alpha=rawr_ridge_alpha,
            available_teams=available_teams,
            source_fingerprint=plan.source_fingerprint,
            build_version=plan.build_version,
        )
        scope_results.append(scope_result)

        _emit_progress(
            event_fn,
            current=index + 1,
            total=len(scopes),
            detail=f"{scope_result.status} {scope.scope_label}",
        )

        if should_fail_empty_rawr_scope:
            failure_message = (
                "RAWR refresh produced no all-teams rows. "
                "The normalized cache is incomplete, so the web store was not updated."
            )
            break

    return RefreshMetricStoreResult(
        metric=normalized_metric,
        scope_results=scope_results,
        warnings=plan.warnings,
        failure_message=failure_message,
    )


def _emit_progress(
    event_fn: MetricStoreRefreshEventFn | None,
    *,
    current: int,
    total: int,
    detail: str,
) -> None:
    if event_fn is None:
        return
    event_fn(
        MetricStoreRefreshProgressEvent(
            current=current,
            total=total,
            detail=detail,
        )
    )


def _prepare_metric_store_refresh(
    metric: Metric,
    *,
    season_type: SeasonType,
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
    include_team_scopes: bool = True,
) -> _MetricStoreRefreshPlan:
    cache_load_rows = _list_cache_load_rows_for_season_type(season_type)
    if not cache_load_rows:
        return _MetricStoreRefreshPlan(
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
    source_fingerprint = build_normalized_cache_fingerprint(season_type=season_type)
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
    return _MetricStoreRefreshPlan(
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        warnings=warnings,
        available_teams=unique_available_teams,
        scopes=scopes,
    )


def _refresh_metric_store_scope(
    *,
    metric: Metric,
    scope: _MetricStoreRefreshScope,
    season_type: SeasonType,
    rawr_ridge_alpha: float,
    available_teams: list[Team],
    source_fingerprint: str,
    build_version: str,
) -> tuple[RefreshScopeResult, bool]:
    state = load_metric_scope_store_state(metric.value, scope.scope_key)
    if (
        state is not None
        and state.snapshot_state.source_fingerprint == source_fingerprint
        and state.snapshot_state.build_version == build_version
        and state.snapshot_state.row_count > 0
    ):
        return (
            RefreshScopeResult(
                scope_key=scope.scope_key,
                scope_label=scope.scope_label,
                row_count=state.snapshot_state.row_count,
                status="cached",
            ),
            False,
        )

    catalog = _build_scope_catalog(
        metric=metric,
        scope=scope,
        season_type=season_type,
        available_teams=available_teams,
    )
    if metric == Metric.RAWR:
        rows = _build_rawr_cached_rows(
            scope_key=scope.scope_key,
            team_filter=scope.catalog.team_filter,
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
            catalog=catalog,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
            rows=rows,
        )
        row_count = len(rows)
    else:
        rows = _build_wowy_cached_rows(
            metric=metric,
            scope_key=scope.scope_key,
            team_filter=scope.catalog.team_filter,
            season_type=season_type,
            teams=scope.teams,
        )
        replace_wowy_scope_snapshot(
            metric_id=metric.value,
            scope_key=scope.scope_key,
            catalog=catalog,
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
) -> _MetricStoreRefreshScope:
    normalized_teams = normalize_teams(teams)
    normalized_team_ids = to_team_ids(normalized_teams)
    team_filter = build_team_filter(normalized_teams)
    scope_key = build_scope_key(season_type=season_type, team_filter=team_filter)
    return _MetricStoreRefreshScope(
        teams=normalized_teams,
        scope_key=scope_key,
        scope_label=(
            normalized_teams[0].current.abbreviation
            if normalized_teams and len(normalized_teams) == 1
            else team_filter or "all-teams"
        ),
        catalog=MetricScopeCatalog(
            label="",
            team_filter=team_filter,
            season_type=season_type.to_nba_format(),
            availability=MetricScopeAvailability(
                season_ids=[
                    season.id
                    for season in sorted(
                        {
                            scope.season
                            for scope in cached_team_seasons
                            if normalized_team_ids is None
                            or scope.team.team_id in normalized_team_ids
                        },
                        key=lambda season: season.id,
                    )
                ],
                team_ids=[],
            ),
            full_span=None,
        ),
    )


def _build_scope_catalog(
    *,
    metric: Metric,
    scope: _MetricStoreRefreshScope,
    season_type: SeasonType,
    available_teams: list[Team],
) -> MetricScopeCatalog:
    season_ids = scope.catalog.availability.season_ids
    return MetricScopeCatalog(
        label=metric.value,
        team_filter=scope.catalog.team_filter,
        season_type=season_type.to_nba_format(),
        availability=MetricScopeAvailability(
            season_ids=season_ids,
            team_ids=[team.team_id for team in available_teams],
        ),
        full_span=_build_metric_season_span_ids(season_ids),
    )


def _build_metric_season_span_ids(season_ids: list[str]) -> MetricSeasonSpanIds | None:
    if not season_ids:
        return None
    return MetricSeasonSpanIds(
        start_season_id=season_ids[0],
        end_season_id=season_ids[-1],
    )


def _build_rawr_cached_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
    teams: list[Team] | None,
    rawr_ridge_alpha: float,
) -> list[RawrPlayerSeasonValueRow]:
    request = load_rawr_request(
        teams=teams,
        seasons=None,
        season_type=season_type,
        min_games=1,
        ridge_alpha=rawr_ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
    )
    records = build_player_season_records(request)
    return [
        RawrPlayerSeasonValueRow(
            snapshot_id=None,
            metric_id="rawr",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type.value,
            season_id=record.season.id,
            player_id=record.player.player_id,
            player_name=record.player.player_name,
            games=record.games,
            coefficient=record.coefficient,
            average_minutes=record.minutes.average_minutes,
            total_minutes=record.minutes.total_minutes,
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
    season_inputs = load_wowy_season_inputs(
        teams=teams,
        seasons=None,
        season_type=season_type,
    )
    records = prepare_wowy_player_season_records(
        season_inputs=season_inputs,
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
            (record.season, record.player.player_id): compute_wowy_shrinkage_score(
                games_with=record.result.games_with,
                games_without=record.result.games_without,
                wowy_score=record.result.value,
                prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
            )
            for record in records
        }
    rows: list[WowyPlayerSeasonValueRow] = []
    for record in records:
        value = (
            values_by_player_season[(record.season, record.player.player_id)]
            if values_by_player_season is not None
            else record.result.value
        )
        rows.append(
            WowyPlayerSeasonValueRow(
                snapshot_id=None,
                metric_id=metric.value,
                scope_key=scope_key,
                team_filter=team_filter,
                season_type=season_type.value,
                season_id=record.season.id,
                player_id=record.player.player_id,
                player_name=record.player.player_name,
                value=value,
                games_with=record.result.games_with,
                games_without=record.result.games_without,
                avg_margin_with=record.result.avg_margin_with,
                avg_margin_without=record.result.avg_margin_without,
                average_minutes=record.minutes.average_minutes,
                total_minutes=record.minutes.total_minutes,
                raw_wowy_score=record.result.value if include_raw_wowy_score else None,
            )
        )
    return rows


def _describe_metric(metric: Metric) -> MetricSummary:
    if metric == Metric.RAWR:
        return RAWR_METRIC_SUMMARY
    return describe_wowy_metric(metric)


def _list_cache_load_rows_for_season_type(season_type: SeasonType) -> list[NormalizedCacheLoadRow]:
    return [row for row in list_cache_load_rows() if row.season.season_type == season_type]


def _list_cached_team_scopes_for_season_type(season_type: SeasonType) -> list[TeamSeasonScope]:
    return [
        scope for scope in list_cached_team_seasons() if scope.season.season_type == season_type
    ]
