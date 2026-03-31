from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from rawr_analytics.data.game_cache import list_cache_load_rows, list_cached_team_seasons
from rawr_analytics.data.game_cache.rows import NormalizedCacheLoadRow
from rawr_analytics.data.metric_store_scope import build_scope_key, build_team_filter
from rawr_analytics.data.player_metrics_db.builders import (
    build_rawr_player_season_metric_rows,
    build_wowy_player_season_metric_rows,
)
from rawr_analytics.data.player_metrics_db.models import (
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
)
from rawr_analytics.data.player_metrics_db.queries import (
    load_metric_scope_catalog_row,
    load_metric_store_metadata,
)
from rawr_analytics.data.player_metrics_db.store import (
    clear_metric_scope_store,
    replace_metric_scope_store,
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
class _RefreshScope:
    teams: list[Team] | None
    scope_key: str
    team_filter: str
    scope_label: str
    available_seasons: list[Season]


def refresh_metric_store(
    metric: Metric,
    *,
    season_type: SeasonType,
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
    include_team_scopes: bool = True,
    progress: RefreshProgressFn | None = None,
) -> RefreshMetricStoreResult:
    cache_load_rows = _list_cache_load_rows_for_season_type(season_type)
    if not cache_load_rows:
        return RefreshMetricStoreResult(
            metric=metric,
            scope_results=[],
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

    scope_results: list[RefreshScopeResult] = []
    failure_message: str | None = None
    for index, teams in enumerate(team_scopes):
        scope = _build_refresh_scope(
            teams=teams,
            season_type=season_type,
            cached_team_seasons=cached_team_seasons,
        )
        if progress is not None:
            progress(index, len(team_scopes), f"building {scope.scope_label}")

        scope_result, should_fail_empty_rawr_scope = _refresh_metric_store_scope(
            metric=metric,
            metric_label=metric_info.label,
            scope=scope,
            season_type=season_type,
            rawr_ridge_alpha=rawr_ridge_alpha,
            available_teams=unique_available_teams,
            source_fingerprint=source_fingerprint,
            build_version=build_version,
        )
        scope_results.append(scope_result)

        if progress is not None:
            progress(index + 1, len(team_scopes), f"{scope_result.status} {scope.scope_label}")

        if should_fail_empty_rawr_scope:
            failure_message = (
                "RAWR refresh produced no all-teams rows. "
                "The normalized cache is incomplete, so the web store was not updated."
            )
            break

    return RefreshMetricStoreResult(
        metric=metric,
        scope_results=scope_results,
        warnings=warnings,
        failure_message=failure_message,
    )


def _refresh_metric_store_scope(
    *,
    metric: Metric,
    metric_label: str,
    scope: _RefreshScope,
    season_type: SeasonType,
    rawr_ridge_alpha: float,
    available_teams: list[Team],
    source_fingerprint: str,
    build_version: str,
) -> tuple[RefreshScopeResult, bool]:
    metadata = load_metric_store_metadata(metric.value, scope.scope_key)
    catalog_row = load_metric_scope_catalog_row(metric.value, scope.scope_key)
    if (
        metadata is not None
        and catalog_row is not None
        and metadata.source_fingerprint == source_fingerprint
        and metadata.build_version == build_version
        and metadata.row_count > 0
    ):
        return (
            RefreshScopeResult(
                scope_key=scope.scope_key,
                scope_label=scope.scope_label,
                row_count=metadata.row_count,
                status="cached",
            ),
            False,
        )

    rows = _build_cached_rows(
        metric=metric,
        scope_key=scope.scope_key,
        team_filter=scope.team_filter,
        season_type=season_type,
        teams=scope.teams,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )
    should_fail_empty_rawr_scope = metric == Metric.RAWR and scope.teams is None and not rows
    if should_fail_empty_rawr_scope:
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

    series_rows, point_rows = _build_metric_full_span_rows(
        rows,
        metric=metric,
        scope_key=scope.scope_key,
        seasons=scope.available_seasons,
    )
    replace_metric_scope_store(
        metric_id=metric.value,
        scope_key=scope.scope_key,
        label=metric_label,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
        catalog_row=MetricScopeCatalogRow(
            metric_id=metric.value,
            scope_key=scope.scope_key,
            label=metric_label,
            team_filter=scope.team_filter,
            season_type=season_type.to_nba_format(),
            available_season_ids=[season.id for season in scope.available_seasons],
            available_team_ids=[team.team_id for team in available_teams],
            full_span_start_season_id=(
                scope.available_seasons[0].id if scope.available_seasons else None
            ),
            full_span_end_season_id=(
                scope.available_seasons[-1].id if scope.available_seasons else None
            ),
            updated_at=datetime.now(UTC).isoformat(),
        ),
        series_rows=series_rows,
        point_rows=point_rows,
    )
    return (
        RefreshScopeResult(
            scope_key=scope.scope_key,
            scope_label=scope.scope_label,
            row_count=len(rows),
            status="built",
        ),
        False,
    )


def _build_refresh_scope(
    *,
    teams: list[Team] | None,
    season_type: SeasonType,
    cached_team_seasons: list[TeamSeasonScope],
) -> _RefreshScope:
    normalized_teams = normalize_teams(teams)
    normalized_team_ids = to_team_ids(normalized_teams)
    team_filter = build_team_filter(normalized_teams)
    scope_key = build_scope_key(season_type=season_type, team_filter=team_filter)
    return _RefreshScope(
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


def _build_cached_rows(
    *,
    metric: Metric,
    scope_key: str,
    team_filter: str,
    season_type: SeasonType,
    teams: list[Team] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    if metric == Metric.RAWR:
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
        return build_rawr_player_season_metric_rows(
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            records=records,
        )

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
    return build_wowy_player_season_metric_rows(
        metric_id=metric.value,
        scope_key=scope_key,
        team_filter=team_filter,
        season_type=season_type,
        records=records,
        values_by_player_season=values_by_player_season,
        include_raw_wowy_score=include_raw_wowy_score,
    )


def _build_metric_full_span_rows(
    rows: list[PlayerSeasonMetricRow],
    *,
    metric: Metric,
    scope_key: str,
    seasons: list[Season],
) -> tuple[list[MetricFullSpanSeriesRow], list[MetricFullSpanPointRow]]:
    totals: dict[int, float] = {}
    counts: dict[int, int] = {}
    names: dict[int, str] = {}
    season_values: dict[int, dict[str, float]] = {}

    for row in rows:
        totals[row.player_id] = totals.get(row.player_id, 0.0) + row.value
        counts[row.player_id] = counts.get(row.player_id, 0) + 1
        names[row.player_id] = row.player_name
        season_values.setdefault(row.player_id, {})[row.season_id] = row.value

    span_length = len(seasons) or 1
    ranked_player_ids = sorted(
        totals,
        key=lambda player_id: (totals[player_id], names[player_id]),
        reverse=True,
    )

    series_rows: list[MetricFullSpanSeriesRow] = []
    point_rows: list[MetricFullSpanPointRow] = []
    for rank_order, player_id in enumerate(ranked_player_ids, start=1):
        series_rows.append(
            MetricFullSpanSeriesRow(
                metric_id=metric.value,
                scope_key=scope_key,
                player_id=player_id,
                player_name=names[player_id],
                span_average_value=totals[player_id] / span_length,
                season_count=counts[player_id],
                rank_order=rank_order,
            )
        )
        for season in seasons:
            value = season_values[player_id].get(season.id)
            if value is None:
                continue
            point_rows.append(
                MetricFullSpanPointRow(
                    metric_id=metric.value,
                    scope_key=scope_key,
                    player_id=player_id,
                    season_id=season.id,
                    value=value,
                )
            )
    return series_rows, point_rows


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
    "RefreshMetricStoreResult",
    "RefreshScopeResult",
    "refresh_metric_store",
]
