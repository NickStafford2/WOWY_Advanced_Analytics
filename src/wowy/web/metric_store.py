from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from wowy.apps.rawr.data import build_rawr_metric_rows, list_incomplete_rawr_seasons
from wowy.apps.wowy.records import (
    build_wowy_metric_rows,
    build_wowy_shrunk_metric_rows,
)
from wowy.data.game_cache import list_cached_team_seasons
from wowy.data.game_cache.fingerprints import build_normalized_cache_fingerprint
from wowy.data.game_cache.repository import list_cache_load_rows
from wowy.data.player_metrics_db import (
    DEFAULT_PLAYER_METRICS_DB_PATH,
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
    clear_metric_scope_store,
    load_metric_scope_catalog_row,
    load_metric_store_metadata,
    replace_metric_scope_store,
)
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.team_history import official_continuity_label_for_team_id

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


WOWY_METRIC = "wowy"
WOWY_SHRUNK_METRIC = "wowy_shrunk"
RAWR_METRIC = "rawr"
DEFAULT_RAWR_RIDGE_ALPHA = 10.0


METRIC_DEFINITIONS = {
    WOWY_METRIC: _MetricDefinition(
        metric=WOWY_METRIC,
        label="WOWY",
        build_version="wowy-player-season-v3",
        build_rows=build_wowy_metric_rows,
    ),
    WOWY_SHRUNK_METRIC: _MetricDefinition(
        metric=WOWY_SHRUNK_METRIC,
        label="WOWY Shrunk",
        build_version="wowy-shrunk-player-season-v1",
        build_rows=build_wowy_shrunk_metric_rows,
    ),
    RAWR_METRIC: _MetricDefinition(
        metric=RAWR_METRIC,
        label="RAWR",
        build_version="rawr-player-season-v3",
        build_rows=build_rawr_metric_rows,
    ),
}


def build_scope_key(
    *,
    team_ids: list[int] | None,
    season_type: str,
) -> tuple[str, str]:
    season_type = canonicalize_season_type(season_type)
    normalized_team_ids = sorted({team_id for team_id in team_ids or [] if team_id > 0})
    team_filter = ",".join(str(team_id) for team_id in normalized_team_ids)
    team_key = team_filter or "all-teams"
    return (
        f"team_ids={team_key}|season_type={season_type}",
        team_filter,
    )


def refresh_metric_store(
    metric: str,
    *,
    season_type: str,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    source_data_dir: Path,
    rawr_ridge_alpha: float = DEFAULT_RAWR_RIDGE_ALPHA,
    include_team_scopes: bool = True,
    progress: RefreshProgressFn | None = None,
) -> RefreshMetricStoreResult:
    season_type = canonicalize_season_type(season_type)
    definition = _get_metric_definition(metric)
    cache_load_rows = list_cache_load_rows(db_path, season_type=season_type)
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

    source_fingerprint = build_normalized_cache_fingerprint(
        db_path,
        season_type=season_type,
    )
    cached_team_seasons = list_cached_team_seasons(
        player_metrics_db_path=db_path,
        season_type=season_type,
    )
    available_teams = sorted({team_season.team for team_season in cached_team_seasons})
    available_team_ids = sorted({team_season.team_id for team_season in cached_team_seasons})
    team_scopes: list[list[int] | None] = [None]
    if include_team_scopes:
        team_scopes.extend([[team_id] for team_id in available_team_ids])

    warnings: list[str] = []
    scope_results: list[RefreshScopeResult] = []
    failure_message: str | None = None

    for index, team_ids in enumerate(team_scopes):
        scope_key, team_filter = build_scope_key(team_ids=team_ids, season_type=season_type)
        scope_label = (
            official_continuity_label_for_team_id(team_ids[0])
            if team_ids and len(team_ids) == 1
            else team_filter or "all-teams"
        )
        if metric == RAWR_METRIC and team_ids is None:
            warnings = _print_rawr_incomplete_season_warning(
                season_type=season_type,
                db_path=db_path,
            )

        scope_seasons = sorted(
            {
                team_season.season
                for team_season in cached_team_seasons
                if team_ids is None or team_season.team_id in team_ids
            }
        )
        if progress is not None:
            progress(index, len(team_scopes), f"building {scope_label}")

        metadata = load_metric_store_metadata(db_path, metric, scope_key)
        catalog_row = load_metric_scope_catalog_row(db_path, metric, scope_key)
        build_version = (
            f"{definition.build_version}-alpha-{rawr_ridge_alpha:.4f}"
            if metric == RAWR_METRIC
            else definition.build_version
        )
        if (
            metadata is not None
            and catalog_row is not None
            and metadata.source_fingerprint == source_fingerprint
            and metadata.build_version == build_version
            and metadata.row_count > 0
        ):
            if progress is not None:
                progress(index + 1, len(team_scopes), f"cached {scope_label}")
            scope_results.append(
                RefreshScopeResult(
                    scope_key=scope_key,
                    scope_label=scope_label,
                    row_count=metadata.row_count,
                    status="cached",
                )
            )
            continue

        rows = definition.build_rows(
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            source_data_dir=source_data_dir,
            db_path=db_path,
            teams=None,
            team_ids=team_ids,
            rawr_ridge_alpha=rawr_ridge_alpha,
        )
        should_fail_empty_rawr_scope = metric == RAWR_METRIC and team_ids is None and not rows
        if should_fail_empty_rawr_scope:
            clear_metric_scope_store(
                db_path,
                metric=definition.metric,
                scope_key=scope_key,
            )
            status = "empty"
            if progress is not None:
                progress(index + 1, len(team_scopes), f"empty {scope_label}")
        else:
            _replace_metric_scope_rows(
                db_path=db_path,
                definition=definition,
                scope_key=scope_key,
                team_filter=team_filter,
                season_type=season_type,
                rows=rows,
                available_teams=available_teams,
                available_seasons=scope_seasons,
                build_version=build_version,
                source_fingerprint=source_fingerprint,
            )
            status = "built"
            if progress is not None:
                progress(index + 1, len(team_scopes), f"built {scope_label}")

        scope_results.append(
            RefreshScopeResult(
                scope_key=scope_key,
                scope_label=scope_label,
                row_count=len(rows),
                status=status,
            )
        )
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


def _print_rawr_incomplete_season_warning(
    *,
    season_type: str,
    db_path: Path,
) -> list[str]:
    cached_team_seasons = list_cached_team_seasons(
        player_metrics_db_path=db_path,
        season_type=season_type,
    )
    candidate_seasons = sorted({team_season.season for team_season in cached_team_seasons})
    if not candidate_seasons:
        return []

    issues = list_incomplete_rawr_seasons(
        seasons=candidate_seasons,
        season_type=season_type,
        player_metrics_db_path=db_path,
    )
    if not issues:
        return []

    print("RAWR warning: skipped incomplete seasons")
    for issue in issues:
        print(f"  - {issue.season}: {issue.reason}")
    return [f"{issue.season}: {issue.reason}" for issue in issues]


def _replace_metric_scope_rows(
    *,
    db_path: Path,
    definition: _MetricDefinition,
    scope_key: str,
    team_filter: str,
    season_type: str,
    rows: list[PlayerSeasonMetricRow],
    available_teams: list[str],
    available_seasons: list[str],
    build_version: str,
    source_fingerprint: str,
) -> None:
    span_series = _build_metric_series(
        rows,
        seasons=available_seasons,
        top_n=len({row.player_id for row in rows}),
    )
    replace_metric_scope_store(
        db_path,
        metric=definition.metric,
        scope_key=scope_key,
        metric_label=definition.label,
        build_version=build_version,
        source_fingerprint=source_fingerprint,
        rows=rows,
        catalog_row=MetricScopeCatalogRow(
            metric=definition.metric,
            scope_key=scope_key,
            metric_label=definition.label,
            team_filter=team_filter,
            season_type=season_type,
            available_seasons=available_seasons,
            available_teams=available_teams,
            full_span_start_season=available_seasons[0] if available_seasons else None,
            full_span_end_season=available_seasons[-1] if available_seasons else None,
            updated_at=datetime.now(UTC).isoformat(),
        ),
        series_rows=[
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
        point_rows=[
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
    )


def _get_metric_definition(metric: str) -> _MetricDefinition:
    try:
        return METRIC_DEFINITIONS[metric]
    except KeyError as exc:
        raise ValueError(f"Unknown metric: {metric}") from exc
