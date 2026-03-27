from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, TypedDict

from rawr_analytics.data.game_cache import list_cached_team_seasons
from rawr_analytics.data.game_cache.fingerprints import build_normalized_cache_fingerprint
from rawr_analytics.data.game_cache.repository import list_cache_load_rows
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
from rawr_analytics.metrics.rawr import build_cached_rows as build_rawr_cached_rows
from rawr_analytics.metrics.rawr import describe_metric as describe_rawr_metric
from rawr_analytics.metrics.rawr.data import list_incomplete_rawr_seasons
from rawr_analytics.metrics.scope import build_scope_key
from rawr_analytics.metrics.wowy import build_cached_rows as build_wowy_cached_rows
from rawr_analytics.metrics.wowy import describe_metric as describe_wowy_metric
from rawr_analytics.shared.season import SeasonType

BuildRowsFn = Callable[..., list[PlayerSeasonMetricRow]]
RefreshProgressFn = Callable[[int, int, str], None]


@dataclass(frozen=True)
class _MetricDefinition:
    metric: str
    label: str
    build_version: str
    build_rows: BuildRowsFn


@dataclass(frozen=True)
class RefreshScopeResult:
    scope_key: str
    scope_label: str
    row_count: int
    status: str


@dataclass(frozen=True)
class RefreshMetricStoreResult:
    metric: str
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
class _RefreshScopeContext:
    team_ids: list[int] | None
    scope_key: str
    team_filter: str
    scope_label: str
    available_seasons: list[str]


@dataclass(frozen=True)
class _RefreshStoreInputs:
    source_fingerprint: str
    cached_team_seasons: list[Any]
    available_team_ids: list[int]
    team_scopes: list[list[int] | None]


class _MetricScopeStoreRows(TypedDict):
    series_rows: list[MetricFullSpanSeriesRow]
    point_rows: list[MetricFullSpanPointRow]


WOWY_METRIC = "wowy"
WOWY_SHRUNK_METRIC = "wowy_shrunk"
RAWR_METRIC = "rawr"
DEFAULT_RAWR_RIDGE_ALPHA = 10.0


METRIC_DEFINITIONS = {
    WOWY_METRIC: _MetricDefinition(
        metric=WOWY_METRIC,
        label=describe_wowy_metric(WOWY_METRIC)["label"],
        build_version=describe_wowy_metric(WOWY_METRIC)["build_version"],
        build_rows=lambda **kwargs: build_wowy_cached_rows(WOWY_METRIC, **kwargs),
    ),
    WOWY_SHRUNK_METRIC: _MetricDefinition(
        metric=WOWY_SHRUNK_METRIC,
        label=describe_wowy_metric(WOWY_SHRUNK_METRIC)["label"],
        build_version=describe_wowy_metric(WOWY_SHRUNK_METRIC)["build_version"],
        build_rows=lambda **kwargs: build_wowy_cached_rows(WOWY_SHRUNK_METRIC, **kwargs),
    ),
    RAWR_METRIC: _MetricDefinition(
        metric=RAWR_METRIC,
        label=describe_rawr_metric(RAWR_METRIC)["label"],
        build_version=describe_rawr_metric(RAWR_METRIC)["build_version"],
        build_rows=build_rawr_cached_rows,
    ),
}


def refresh_metric_store(
    metric: str,
    *,
    season_type: SeasonType,
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
    include_team_scopes: bool = True,
    progress: RefreshProgressFn | None = None,
) -> RefreshMetricStoreResult:
    definition = _get_metric_definition(metric)
    if not _has_refreshable_cache(season_type=season_type):
        return _build_empty_cache_refresh_result(metric)

    store_inputs = _build_refresh_store_inputs(
        season_type=season_type,
        include_team_scopes=include_team_scopes,
    )
    warnings = _build_refresh_warnings(
        metric=metric,
        season_type=season_type,
    )
    scope_results: list[RefreshScopeResult] = []
    failure_message: str | None = None
    build_version = (
        f"{definition.build_version}-alpha-{rawr_ridge_alpha:.4f}"
        if metric == RAWR_METRIC
        else definition.build_version
    )

    for index, team_ids in enumerate(store_inputs.team_scopes):
        scope = _build_refresh_scope_context(
            team_ids=team_ids,
            season_type=season_type,
            cached_team_seasons=store_inputs.cached_team_seasons,
        )

        if progress is not None:
            progress(index, len(store_inputs.team_scopes), f"building {scope.scope_label}")

        scope_result, should_fail_empty_rawr_scope = _refresh_metric_store_scope(
            definition=definition,
            metric=metric,
            scope=scope,
            season_type=season_type,
            rawr_ridge_alpha=rawr_ridge_alpha,
            available_team_ids=store_inputs.available_teams_ids,
            source_fingerprint=store_inputs.source_fingerprint,
            build_version=build_version,
        )
        if progress is not None:
            progress(
                index + 1,
                len(store_inputs.team_scopes),
                f"{scope_result.status} {scope.scope_label}",
            )
        scope_results.append(scope_result)
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


def _has_refreshable_cache(*, season_type: SeasonType) -> bool:
    return bool(list_cache_load_rows(season_type=season_type))


def _build_empty_cache_refresh_result(metric: str) -> RefreshMetricStoreResult:
    return RefreshMetricStoreResult(
        metric=metric,
        scope_results=[],
        warnings=[],
        failure_message=(
            "Normalized cache is empty for the requested season type. "
            "Rebuild ingest before refreshing the web metric store."
        ),
    )


def _build_refresh_store_inputs(
    *,
    season_type: str,
    include_team_scopes: bool,
) -> _RefreshStoreInputs:
    cached_team_seasons = list_cached_team_seasons(
        season_type=season_type,
    )
    available_team_ids = sorted({team_season.team_id for team_season in cached_team_seasons})
    team_scopes: list[list[int] | None] = [None]
    if include_team_scopes:
        team_scopes.extend([[team_id] for team_id in available_team_ids])
    return _RefreshStoreInputs(
        source_fingerprint=build_normalized_cache_fingerprint(
            season_type=season_type,
        ),
        cached_team_seasons=cached_team_seasons,
        available_team_ids=sorted({ts.team_id for ts in cached_team_seasons}),
        team_scopes=team_scopes,
    )


def _build_refresh_warnings(
    *,
    metric: str,
    season_type: str,
) -> list[str]:
    if metric != RAWR_METRIC:
        return []
    return _print_rawr_incomplete_season_warning(season_type=season_type)


def _build_refresh_scope_context(
    *,
    team_ids: list[int] | None,
    season_type: str,
    cached_team_seasons,
) -> _RefreshScopeContext:
    scope_key, team_filter = build_scope_key(team_ids=team_ids, season_type=season_type)
    return _RefreshScopeContext(
        team_ids=team_ids,
        scope_key=scope_key,
        team_filter=team_filter,
        scope_label=(
            official_continuity_label_for_team_id(team_ids[0])
            if team_ids and len(team_ids) == 1
            else team_filter or "all-teams"
        ),
        available_seasons=sorted(
            {
                team_season.season
                for team_season in cached_team_seasons
                if team_ids is None or team_season.team_id in team_ids
            }
        ),
    )


def _refresh_metric_store_scope(
    *,
    definition: _MetricDefinition,
    metric: str,
    scope: _RefreshScopeContext,
    season_type: str,
    rawr_ridge_alpha: float,
    available_team_ids: list[int],
    source_fingerprint: str,
    build_version: str,
) -> tuple[RefreshScopeResult, bool]:
    metadata = load_metric_store_metadata(metric, scope.scope_key)
    catalog_row = load_metric_scope_catalog_row(metric, scope.scope_key)
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

    rows = definition.build_rows(
        scope_key=scope.scope_key,
        team_filter=scope.team_filter,
        season_type=season_type,
        teams=None,
        team_ids=scope.team_ids,
        rawr_ridge_alpha=rawr_ridge_alpha,
    )
    should_fail_empty_rawr_scope = metric == RAWR_METRIC and scope.team_ids is None and not rows
    if should_fail_empty_rawr_scope:
        clear_metric_scope_store(
            metric=definition.metric,
            scope_key=scope.scope_key,
        )
        status = "empty"
    else:
        _replace_metric_scope_rows(
            definition=definition,
            scope_key=scope.scope_key,
            team_filter=scope.team_filter,
            season_type=season_type,
            rows=rows,
            available_team_ids=available_team_ids,
            available_seasons=scope.available_seasons,
            build_version=build_version,
            source_fingerprint=source_fingerprint,
        )
        status = "built"

    return (
        RefreshScopeResult(
            scope_key=scope.scope_key,
            scope_label=scope.scope_label,
            row_count=len(rows),
            status=status,
        ),
        should_fail_empty_rawr_scope,
    )


def _build_metric_series(
    rows: list[PlayerSeasonMetricRow],
    *,
    seasons: list[str],
    top_n: int,
) -> list[dict[str, Any]]:
    totals: dict[int, float] = {}
    counts: dict[int, int] = {}
    names: dict[int, str] = {}
    season_values: dict[int, dict[str, float]] = {}

    for row in rows:
        totals[row.player_id] = totals.get(row.player_id, 0.0) + row.value
        counts[row.player_id] = counts.get(row.player_id, 0) + 1
        names[row.player_id] = row.player_name
        season_values.setdefault(row.player_id, {})[row.season] = row.value

    span_length = len(seasons)
    ranked_player_ids = sorted(
        totals,
        key=lambda player_id: (totals[player_id], names[player_id]),
        reverse=True,
    )[:top_n]

    return [
        {
            "player_id": player_id,
            "player_name": names[player_id],
            "span_average_value": totals[player_id] / span_length,
            "season_count": counts[player_id],
            "points": [
                {
                    "season": season,
                    "value": season_values[player_id].get(season),
                }
                for season in seasons
            ],
        }
        for player_id in ranked_player_ids
    ]


def _print_rawr_incomplete_season_warning(season_type: str) -> list[str]:
    cached_team_seasons = list_cached_team_seasons(season_type=season_type)
    candidate_seasons = sorted({team_season.season for team_season in cached_team_seasons})
    if not candidate_seasons:
        return []

    issues = list_incomplete_rawr_seasons(
        seasons=candidate_seasons,
        season_type=season_type,
    )
    if not issues:
        return []

    print("RAWR warning: skipped incomplete seasons")
    for issue in issues:
        print(f"  - {issue.season}: {issue.reason}")
    return [f"{issue.season}: {issue.reason}" for issue in issues]


def _replace_metric_scope_rows(
    *,
    definition: _MetricDefinition,
    scope_key: str,
    team_filter: str,
    season_type: str,
    rows: list[PlayerSeasonMetricRow],
    available_team_ids: list[int],
    available_seasons: list[str],
    build_version: str,
    source_fingerprint: str,
) -> None:
    store_rows = _build_metric_scope_store_rows(
        rows,
        definition=definition,
        scope_key=scope_key,
        seasons=available_seasons,
    )
    replace_metric_scope_store(
        metric=definition.metric,
        scope_key=scope_key,
        metric_label=definition.label,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
        catalog_row=_build_metric_scope_catalog_row(
            definition=definition,
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            available_seasons=available_seasons,
            available_team_ids=available_team_ids,
        ),
        series_rows=store_rows["series_rows"],
        point_rows=store_rows["point_rows"],
    )


def _build_metric_scope_catalog_row(
    *,
    definition: _MetricDefinition,
    scope_key: str,
    team_filter: str,
    season_type: str,
    available_seasons: list[str],
    available_team_ids: list[int],
) -> MetricScopeCatalogRow:
    return MetricScopeCatalogRow(
        metric=definition.metric,
        scope_key=scope_key,
        metric_label=definition.label,
        team_filter=team_filter,
        season_type=season_type,
        available_seasons=available_seasons,
        available_team_ids=available_team_ids,
        full_span_start_season=available_seasons[0] if available_seasons else None,
        full_span_end_season=available_seasons[-1] if available_seasons else None,
        updated_at=datetime.now(UTC).isoformat(),
    )


def _build_metric_scope_store_rows(
    rows: list[PlayerSeasonMetricRow],
    *,
    definition: _MetricDefinition,
    scope_key: str,
    seasons: list[str],
) -> _MetricScopeStoreRows:
    span_series = _build_metric_series(
        rows,
        seasons=seasons,
        top_n=len({row.player_id for row in rows}),
    )
    return {
        "series_rows": [
            MetricFullSpanSeriesRow(
                metric=definition.metric,
                scope_key=scope_key,
                player_id=series["player_id"],
                player_name=series["player_name"],
                span_average_value=series["span_average_value"],
                season_count=series["season_count"],
                rank_order=index + 1,
            )
            for index, series in enumerate(span_series)
        ],
        "point_rows": [
            MetricFullSpanPointRow(
                metric=definition.metric,
                scope_key=scope_key,
                player_id=series["player_id"],
                season=point["season"],
                value=point["value"],
            )
            for series in span_series
            for point in series["points"]
            if point["value"] is not None
        ],
    }


def _get_metric_definition(metric: str) -> _MetricDefinition:
    return METRIC_DEFINITIONS[metric]
