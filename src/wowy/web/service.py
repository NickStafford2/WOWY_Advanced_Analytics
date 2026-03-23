from __future__ import annotations
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from wowy.apps.rawr.models import RawrPlayerSeasonRecord
from wowy.apps.rawr.service import (
    list_incomplete_rawr_seasons,
    prepare_rawr_player_season_records,
)
from wowy.apps.wowy.analysis import (
    DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
    compute_wowy_shrinkage_score,
)
from wowy.apps.wowy.models import WowyPlayerSeasonRecord
from wowy.apps.wowy.service import prepare_wowy_player_season_records
from wowy.data.game_cache_db import build_normalized_cache_fingerprint, list_cache_load_rows
from wowy.data.player_metrics_db import (
    DEFAULT_PLAYER_METRICS_DB_PATH,
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
    clear_metric_scope_store,
    load_metric_rows,
    load_metric_full_span_points_map,
    load_metric_full_span_series_rows,
    load_metric_scope_catalog_row,
    load_metric_store_metadata,
    replace_metric_scope_store,
)
from wowy.nba.team_seasons import list_cached_team_seasons
from wowy.nba.season_types import canonicalize_season_type


BuildRowsFn = Callable[..., list[PlayerSeasonMetricRow]]
RefreshProgressFn = Callable[[int, int, str], None]


@dataclass(frozen=True)
class MetricDefinition:
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
DEFAULT_RAWR_SHRINKAGE_MODE = "uniform"
DEFAULT_RAWR_SHRINKAGE_STRENGTH = 1.0
DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE = 48.0


def _build_wowy_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    source_data_dir: Path,
    db_path: Path,
    teams: list[str] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    records = prepare_wowy_player_season_records(
        teams=teams,
        seasons=None,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=db_path,
        min_games_with=0,
        min_games_without=0,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return [
        PlayerSeasonMetricRow(
            metric=WOWY_METRIC,
            metric_label="WOWY",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            season=record.season,
            player_id=record.player_id,
            player_name=record.player_name,
            value=record.wowy_score,
            sample_size=record.games_with,
            secondary_sample_size=record.games_without,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
            details={
                "games_with": record.games_with,
                "games_without": record.games_without,
                "avg_margin_with": record.avg_margin_with,
                "avg_margin_without": record.avg_margin_without,
            },
        )
        for record in records
    ]


def _build_rawr_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    source_data_dir: Path,
    db_path: Path,
    teams: list[str] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    records = prepare_rawr_player_season_records(
        teams=teams,
        seasons=None,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=db_path,
        min_games=1,
        ridge_alpha=rawr_ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return [
        PlayerSeasonMetricRow(
            metric=RAWR_METRIC,
            metric_label="RAWR",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            season=record.season,
            player_id=record.player_id,
            player_name=record.player_name,
            value=record.coefficient,
            sample_size=record.games,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
            details={
                "games": record.games,
            },
        )
        for record in records
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


def _build_wowy_shrunk_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    source_data_dir: Path,
    db_path: Path,
    teams: list[str] | None,
    rawr_ridge_alpha: float,
) -> list[PlayerSeasonMetricRow]:
    records = prepare_wowy_player_season_records(
        teams=teams,
        seasons=None,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=db_path,
        min_games_with=0,
        min_games_without=0,
        min_average_minutes=None,
        min_total_minutes=None,
    )
    return [
        PlayerSeasonMetricRow(
            metric=WOWY_SHRUNK_METRIC,
            metric_label="WOWY Shrunk",
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            season=record.season,
            player_id=record.player_id,
            player_name=record.player_name,
            value=compute_wowy_shrinkage_score(
                games_with=record.games_with,
                games_without=record.games_without,
                wowy_score=record.wowy_score,
                prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
            ),
            sample_size=record.games_with,
            secondary_sample_size=record.games_without,
            average_minutes=record.average_minutes,
            total_minutes=record.total_minutes,
            details={
                "games_with": record.games_with,
                "games_without": record.games_without,
                "avg_margin_with": record.avg_margin_with,
                "avg_margin_without": record.avg_margin_without,
                "raw_wowy_score": record.wowy_score,
            },
        )
        for record in records
    ]


METRIC_DEFINITIONS = {
    WOWY_METRIC: MetricDefinition(
        metric=WOWY_METRIC,
        label="WOWY",
        build_version="wowy-player-season-v3",
        build_rows=_build_wowy_rows,
    ),
    WOWY_SHRUNK_METRIC: MetricDefinition(
        metric=WOWY_SHRUNK_METRIC,
        label="WOWY Shrunk",
        build_version="wowy-shrunk-player-season-v1",
        build_rows=_build_wowy_shrunk_rows,
    ),
    RAWR_METRIC: MetricDefinition(
        metric=RAWR_METRIC,
        label="RAWR",
        build_version="rawr-player-season-v3",
        build_rows=_build_rawr_rows,
    ),
}


def build_scope_key(
    *,
    teams: list[str] | None,
    season_type: str,
) -> tuple[str, str]:
    season_type = canonicalize_season_type(season_type)
    normalized_teams = sorted({team.upper() for team in teams or []})
    team_filter = ",".join(normalized_teams)
    team_key = team_filter or "all-teams"
    return (
        f"teams={team_key}|season_type={season_type}",
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
    definition = get_metric_definition(metric)
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
    cached_team_seasons = list_cached_team_seasons(player_metrics_db_path=db_path, season_type=season_type)
    available_teams = sorted({team_season.team for team_season in cached_team_seasons})
    team_scopes: list[list[str] | None] = [None]
    if include_team_scopes:
        team_scopes.extend([[team] for team in available_teams])
    warnings: list[str] = []
    scope_results: list[RefreshScopeResult] = []
    failure_message: str | None = None

    for index, teams in enumerate(team_scopes):
        scope_key, team_filter = build_scope_key(teams=teams, season_type=season_type)
        scope_label = team_filter or "all-teams"
        if metric == RAWR_METRIC and teams is None:
            warnings = _print_rawr_incomplete_season_warning(
                season_type=season_type,
                db_path=db_path,
            )
        scope_seasons = sorted(
            {
                team_season.season
                for team_season in cached_team_seasons
                if teams is None or team_season.team in teams
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
            teams=teams,
            rawr_ridge_alpha=rawr_ridge_alpha,
        )
        should_fail_empty_rawr_scope = metric == RAWR_METRIC and teams is None and not rows
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


def build_metric_player_seasons_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_sample_size: int | None,
    min_secondary_sample_size: int | None,
) -> dict[str, Any]:
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    rows = load_metric_rows(
        db_path,
        metric=metric,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_sample_size=min_sample_size,
        min_secondary_sample_size=min_secondary_sample_size,
    )
    definition = get_metric_definition(metric)
    return {
        "metric": metric,
        "metric_label": definition.label,
        "rows": [serialize_metric_player_season_row(row) for row in rows],
    }


def build_cached_metric_leaderboard_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    scope_key: str,
    top_n: int,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_sample_size: int | None,
    min_secondary_sample_size: int | None,
) -> dict[str, Any]:
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    rows = load_metric_rows(
        db_path,
        metric=metric,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_sample_size=min_sample_size,
        min_secondary_sample_size=min_secondary_sample_size,
    )
    leaderboard = build_leaderboard_payload_from_rows(
        metric=metric,
        metric_label=catalog_row.metric_label,
        rows=rows,
        seasons=seasons or catalog_row.available_seasons,
        top_n=top_n,
        mode="cached",
    )
    leaderboard["available_seasons"] = catalog_row.available_seasons
    leaderboard["available_teams"] = catalog_row.available_teams
    return leaderboard


def build_cached_metric_export_table_rows(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    scope_key: str,
    seasons: list[str] | None,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
    min_sample_size: int | None,
    min_secondary_sample_size: int | None,
) -> tuple[str, list[dict[str, Any]]]:
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    rows = load_metric_rows(
        db_path,
        metric=metric,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
        min_sample_size=min_sample_size,
        min_secondary_sample_size=min_secondary_sample_size,
    )
    table_rows = build_ranked_table_rows(
        [serialize_metric_player_season_row(row) for row in rows],
        seasons=seasons or catalog_row.available_seasons,
        top_n=None,
    )
    return catalog_row.metric_label, table_rows


def build_custom_wowy_leaderboard_payload(
    *,
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str,
    top_n: int,
    source_data_dir: Path,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    records = prepare_wowy_player_season_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=player_metrics_db_path,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return build_leaderboard_payload_from_records(
        metric=WOWY_METRIC,
        metric_label="WOWY",
        records=records,
        seasons=sorted({record.season for record in records}),
        top_n=top_n,
        mode="custom",
    )


def build_custom_wowy_shrunk_leaderboard_payload(
    *,
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str,
    top_n: int,
    source_data_dir: Path,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    records = prepare_wowy_player_season_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=player_metrics_db_path,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    rows = [
        {
            "season": record.season,
            "player_id": record.player_id,
            "player_name": record.player_name,
            "value": compute_wowy_shrinkage_score(
                games_with=record.games_with,
                games_without=record.games_without,
                wowy_score=record.wowy_score,
                prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
            ),
            "sample_size": record.games_with,
            "secondary_sample_size": record.games_without,
            "games_with": record.games_with,
            "games_without": record.games_without,
            "avg_margin_with": record.avg_margin_with,
            "avg_margin_without": record.avg_margin_without,
            "average_minutes": record.average_minutes,
            "total_minutes": record.total_minutes,
            "raw_wowy_score": record.wowy_score,
        }
        for record in records
    ]
    table_rows = build_ranked_table_rows(
        rows,
        seasons=sorted({record.season for record in records}),
        top_n=top_n,
    )
    seasons_in_scope = sorted({record.season for record in records})
    return {
        "mode": "custom",
        "metric": WOWY_SHRUNK_METRIC,
        "metric_label": "WOWY Shrunk",
        "span": build_span_payload(seasons_in_scope, top_n=top_n),
        "table_rows": table_rows,
        "series": build_series_from_table_rows(table_rows, seasons=seasons_in_scope),
    }


def build_custom_rawr_leaderboard_payload(
    *,
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str,
    top_n: int,
    source_data_dir: Path,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_games: int,
    ridge_alpha: float,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    records = prepare_rawr_player_season_records(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=player_metrics_db_path,
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
        shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
        shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return build_leaderboard_payload_from_rawr_records(
        metric=RAWR_METRIC,
        metric_label="RAWR",
        records=records,
        seasons=sorted({record.season for record in records}),
        top_n=top_n,
        mode="custom",
    )


def build_custom_metric_export_table_rows(
    metric: str,
    *,
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str,
    source_data_dir: Path,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    min_games_with: int | None = None,
    min_games_without: int | None = None,
    min_games: int | None = None,
    ridge_alpha: float | None = None,
    min_average_minutes: float | None = None,
    min_total_minutes: float | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    if metric == WOWY_METRIC:
        records = prepare_wowy_player_season_records(
            teams=teams,
            seasons=seasons,
            season_type=season_type,
            source_data_dir=source_data_dir,
            player_metrics_db_path=player_metrics_db_path,
            min_games_with=int(min_games_with or 0),
            min_games_without=int(min_games_without or 0),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        rows = [
            {
                "season": record.season,
                "player_id": record.player_id,
                "player_name": record.player_name,
                "value": record.wowy_score,
                "sample_size": record.games_with,
                "secondary_sample_size": record.games_without,
                "games_with": record.games_with,
                "games_without": record.games_without,
                "avg_margin_with": record.avg_margin_with,
                "avg_margin_without": record.avg_margin_without,
                "average_minutes": record.average_minutes,
                "total_minutes": record.total_minutes,
            }
            for record in records
        ]
        seasons_in_scope = sorted({record.season for record in records})
        return (
            "WOWY",
            build_ranked_table_rows(rows, seasons=seasons_in_scope, top_n=None),
        )
    if metric == WOWY_SHRUNK_METRIC:
        records = prepare_wowy_player_season_records(
            teams=teams,
            seasons=seasons,
            season_type=season_type,
            source_data_dir=source_data_dir,
            player_metrics_db_path=player_metrics_db_path,
            min_games_with=int(min_games_with or 0),
            min_games_without=int(min_games_without or 0),
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        rows = [
            {
                "season": record.season,
                "player_id": record.player_id,
                "player_name": record.player_name,
                "value": compute_wowy_shrinkage_score(
                    games_with=record.games_with,
                    games_without=record.games_without,
                    wowy_score=record.wowy_score,
                    prior_games=DEFAULT_WOWY_SHRINKAGE_PRIOR_GAMES,
                ),
                "sample_size": record.games_with,
                "secondary_sample_size": record.games_without,
                "games_with": record.games_with,
                "games_without": record.games_without,
                "avg_margin_with": record.avg_margin_with,
                "avg_margin_without": record.avg_margin_without,
                "average_minutes": record.average_minutes,
                "total_minutes": record.total_minutes,
                "raw_wowy_score": record.wowy_score,
            }
            for record in records
        ]
        seasons_in_scope = sorted({record.season for record in records})
        return (
            "WOWY Shrunk",
            build_ranked_table_rows(rows, seasons=seasons_in_scope, top_n=None),
        )
    if metric == RAWR_METRIC:
        records = prepare_rawr_player_season_records(
            teams=teams,
            seasons=seasons,
            season_type=season_type,
            source_data_dir=source_data_dir,
            player_metrics_db_path=player_metrics_db_path,
            min_games=int(min_games or 0),
            ridge_alpha=float(ridge_alpha or DEFAULT_RAWR_RIDGE_ALPHA),
            shrinkage_mode=DEFAULT_RAWR_SHRINKAGE_MODE,
            shrinkage_strength=DEFAULT_RAWR_SHRINKAGE_STRENGTH,
            shrinkage_minute_scale=DEFAULT_RAWR_SHRINKAGE_MINUTE_SCALE,
            min_average_minutes=min_average_minutes,
            min_total_minutes=min_total_minutes,
        )
        rows = [
            {
                "season": record.season,
                "player_id": record.player_id,
                "player_name": record.player_name,
                "value": record.coefficient,
                "sample_size": record.games,
                "secondary_sample_size": None,
                "games": record.games,
                "average_minutes": record.average_minutes,
                "total_minutes": record.total_minutes,
            }
            for record in records
        ]
        seasons_in_scope = sorted({record.season for record in records})
        return (
            "RAWR",
            build_ranked_table_rows(rows, seasons=seasons_in_scope, top_n=None),
        )
    raise ValueError(f"Unknown metric: {metric}")


def build_metric_options_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    teams: list[str] | None,
    season_type: str,
) -> dict[str, Any]:
    scope_key, _team_filter = build_scope_key(teams=teams, season_type=season_type)
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    return {
        "metric": catalog_row.metric,
        "metric_label": catalog_row.metric_label,
        "available_teams": catalog_row.available_teams,
        "available_seasons": catalog_row.available_seasons,
        "filters": build_metric_default_filters_payload(
            metric,
            teams=sorted({team.upper() for team in teams or []}) or None,
            season_type=catalog_row.season_type,
        ),
    }


def build_metric_default_filters_payload(
    metric: str,
    *,
    teams: list[str] | None,
    season_type: str,
) -> dict[str, Any]:
    season_type = canonicalize_season_type(season_type)
    payload = {
        "team": teams,
        "season_type": season_type,
        "min_average_minutes": 30.0,
        "min_total_minutes": 600.0,
        "top_n": 30,
    }
    if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}:
        payload["min_games_with"] = 15
        payload["min_games_without"] = 2
        return payload
    if metric == RAWR_METRIC:
        payload["min_games"] = 35
        payload["ridge_alpha"] = DEFAULT_RAWR_RIDGE_ALPHA
        return payload
    raise ValueError(f"Unknown metric: {metric}")


def build_metric_span_chart_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    scope_key: str,
    top_n: int,
) -> dict[str, Any]:
    catalog_row = _require_current_metric_scope(
        db_path=db_path,
        metric=metric,
        scope_key=scope_key,
    )
    series_rows = load_metric_full_span_series_rows(
        db_path,
        metric=metric,
        scope_key=scope_key,
        top_n=top_n,
    )
    player_ids = [row.player_id for row in series_rows]
    season_points = load_metric_full_span_points_map(
        db_path,
        metric=metric,
        scope_key=scope_key,
        player_ids=player_ids,
    )

    return {
        "metric": metric,
        "metric_label": catalog_row.metric_label,
        "span": {
            "start_season": catalog_row.full_span_start_season,
            "end_season": catalog_row.full_span_end_season,
            "available_seasons": catalog_row.available_seasons,
            "top_n": top_n,
        },
        "series": [
            {
                "player_id": row.player_id,
                "player_name": row.player_name,
                "span_average_value": row.span_average_value,
                "season_count": row.season_count,
                "points": [
                    {
                        "season": season,
                        "value": season_points.get(row.player_id, {}).get(season),
                    }
                    for season in catalog_row.available_seasons
                ],
            }
            for row in series_rows
        ],
    }


def serialize_metric_player_season_row(row: PlayerSeasonMetricRow) -> dict[str, Any]:
    payload = {
        "season": row.season,
        "player_id": row.player_id,
        "player_name": row.player_name,
        "value": row.value,
        "sample_size": row.sample_size,
        "secondary_sample_size": row.secondary_sample_size,
        "average_minutes": row.average_minutes,
        "total_minutes": row.total_minutes,
    }
    payload.update(row.details or {})
    return payload


def build_leaderboard_payload_from_rows(
    *,
    metric: str,
    metric_label: str,
    rows: list[PlayerSeasonMetricRow],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    table_rows = build_ranked_table_rows(
        [serialize_metric_player_season_row(row) for row in rows],
        seasons=seasons,
        top_n=top_n,
    )
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": build_span_payload(seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": build_series_from_table_rows(table_rows, seasons=seasons),
    }


def build_leaderboard_payload_from_records(
    *,
    metric: str,
    metric_label: str,
    records: list[WowyPlayerSeasonRecord],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    rows = [
        {
            "season": record.season,
            "player_id": record.player_id,
            "player_name": record.player_name,
            "value": record.wowy_score,
            "sample_size": record.games_with,
            "secondary_sample_size": record.games_without,
            "games_with": record.games_with,
            "games_without": record.games_without,
            "avg_margin_with": record.avg_margin_with,
            "avg_margin_without": record.avg_margin_without,
            "average_minutes": record.average_minutes,
            "total_minutes": record.total_minutes,
        }
        for record in records
    ]
    table_rows = build_ranked_table_rows(rows, seasons=seasons, top_n=top_n)
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": build_span_payload(seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": build_series_from_table_rows(table_rows, seasons=seasons),
    }


def build_leaderboard_payload_from_rawr_records(
    *,
    metric: str,
    metric_label: str,
    records: list[RawrPlayerSeasonRecord],
    seasons: list[str],
    top_n: int,
    mode: str,
) -> dict[str, Any]:
    rows = [
        {
            "season": record.season,
            "player_id": record.player_id,
            "player_name": record.player_name,
            "value": record.coefficient,
            "sample_size": record.games,
            "secondary_sample_size": None,
            "games": record.games,
            "average_minutes": record.average_minutes,
            "total_minutes": record.total_minutes,
        }
        for record in records
    ]
    table_rows = build_ranked_table_rows(rows, seasons=seasons, top_n=top_n)
    return {
        "mode": mode,
        "metric": metric,
        "metric_label": metric_label,
        "span": build_span_payload(seasons, top_n=top_n),
        "table_rows": table_rows,
        "series": build_series_from_table_rows(table_rows, seasons=seasons),
    }


def build_span_payload(seasons: list[str], *, top_n: int) -> dict[str, Any]:
    ordered_seasons = sorted(dict.fromkeys(seasons))
    return {
        "start_season": ordered_seasons[0] if ordered_seasons else None,
        "end_season": ordered_seasons[-1] if ordered_seasons else None,
        "available_seasons": ordered_seasons,
        "top_n": top_n,
    }


def build_ranked_table_rows(
    rows: list[dict[str, Any]],
    *,
    seasons: list[str],
    top_n: int | None,
) -> list[dict[str, Any]]:
    rows_by_player: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_player.setdefault(row["player_id"], []).append(row)

    full_span_length = len(sorted(dict.fromkeys(seasons))) or 1
    ranked_rows = []
    for player_id, player_rows in rows_by_player.items():
        player_name = player_rows[0]["player_name"]
        games_with = sum(
            (row.get("games_with") or row.get("sample_size") or 0)
            for row in player_rows
        )
        games_without = sum(
            (row.get("games_without") or row.get("secondary_sample_size") or 0)
            for row in player_rows
        )
        total_minutes = sum((row.get("total_minutes") or 0.0) for row in player_rows)
        average_minutes = total_minutes / games_with if games_with > 0 else None
        ranked_rows.append(
            {
                "rank": 0,
                "player_id": player_id,
                "player_name": player_name,
                "span_average_value": sum(row["value"] for row in player_rows)
                / full_span_length,
                "average_minutes": average_minutes,
                "total_minutes": total_minutes,
                "games_with": games_with,
                "games_without": games_without,
                "avg_margin_with": weighted_average_rows(
                    player_rows,
                    value_key="avg_margin_with",
                    weight_keys=("games_with", "sample_size"),
                ),
                "avg_margin_without": weighted_average_rows(
                    player_rows,
                    value_key="avg_margin_without",
                    weight_keys=("games_without", "secondary_sample_size"),
                ),
                "season_count": len(player_rows),
                "points": [
                    {
                        "season": season,
                        "value": next(
                            (
                                row["value"]
                                for row in player_rows
                                if row["season"] == season
                            ),
                            None,
                        ),
                    }
                    for season in sorted(dict.fromkeys(seasons))
                ],
            }
        )

    ranked_rows.sort(
        key=lambda row: (row["span_average_value"], row["player_name"]),
        reverse=True,
    )
    limited_rows = ranked_rows if top_n is None else ranked_rows[:top_n]
    return [{**row, "rank": index + 1} for index, row in enumerate(limited_rows)]


def build_series_from_table_rows(
    table_rows: list[dict[str, Any]],
    *,
    seasons: list[str],
) -> list[dict[str, Any]]:
    season_order = sorted(dict.fromkeys(seasons))
    rows_by_player = {row["player_id"]: row for row in table_rows}
    series = []
    for row in table_rows:
        points = row.get("points")
        if points is None:
            points = [{"season": season, "value": None} for season in season_order]
        series.append(
            {
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "span_average_value": row["span_average_value"],
                "season_count": row["season_count"],
                "points": rows_by_player.get(row["player_id"], {}).get(
                    "points",
                    [{"season": season, "value": None} for season in season_order],
                ),
            }
        )
    return series


def weighted_average_rows(
    rows: list[dict[str, Any]],
    *,
    value_key: str,
    weight_keys: tuple[str, str],
) -> float | None:
    weighted_total = 0.0
    weight_total = 0
    for row in rows:
        value = row.get(value_key)
        weight = row.get(weight_keys[0]) or row.get(weight_keys[1]) or 0
        if value is None or weight <= 0:
            continue
        weighted_total += value * weight
        weight_total += weight
    if weight_total == 0:
        return None
    return weighted_total / weight_total


def build_metric_series(
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
        key=lambda player_id: (
            totals[player_id],
            names[player_id],
        ),
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


def _replace_metric_scope_rows(
    *,
    db_path: Path,
    definition: MetricDefinition,
    scope_key: str,
    team_filter: str,
    season_type: str,
    rows: list[PlayerSeasonMetricRow],
    available_teams: list[str],
    available_seasons: list[str],
    build_version: str,
    source_fingerprint: str,
) -> None:
    span_series = build_metric_series(
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


def _require_current_metric_scope(
    *,
    db_path: Path,
    metric: str,
    scope_key: str,
) -> MetricScopeCatalogRow:
    catalog_row = load_metric_scope_catalog_row(db_path, metric, scope_key)
    if catalog_row is None:
        raise ValueError("Metric store has not been built for the requested scope")
    metadata = load_metric_store_metadata(db_path, metric, scope_key)
    if metadata is None:
        raise ValueError("Metric store metadata is missing for the requested scope")
    cache_load_rows = list_cache_load_rows(db_path, season_type=catalog_row.season_type)
    if not cache_load_rows:
        raise ValueError(
            "Normalized cache is empty for the requested scope season type. "
            "Rebuild ingest before using cached metrics."
        )
    current_fingerprint = build_normalized_cache_fingerprint(
        db_path,
        season_type=catalog_row.season_type,
    )
    if metadata.source_fingerprint != current_fingerprint:
        raise ValueError(
            "Cached metric store is stale relative to normalized cache. "
            "Refresh the web metric store after ingest is rebuilt."
        )
    return catalog_row


def get_metric_definition(metric: str) -> MetricDefinition:
    try:
        return METRIC_DEFINITIONS[metric]
    except KeyError as exc:
        raise ValueError(f"Unknown metric: {metric}") from exc
