from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from wowy.apps.wowy.service import prepare_wowy_player_season_records
from wowy.data.player_metrics_db import (
    DEFAULT_PLAYER_METRICS_DB_PATH,
    MetricFullSpanPointRow,
    MetricFullSpanSeriesRow,
    MetricScopeCatalogRow,
    PlayerSeasonMetricRow,
    load_metric_rows,
    load_metric_full_span_points_map,
    load_metric_full_span_series_rows,
    load_metric_scope_catalog_row,
    load_metric_store_metadata,
    replace_metric_full_span_rows,
    replace_metric_scope_catalog_row,
    replace_metric_rows,
)
from wowy.nba.team_seasons import list_cached_team_seasons


BuildRowsFn = Callable[..., list[PlayerSeasonMetricRow]]
RefreshProgressFn = Callable[[int, int, str], None]


@dataclass(frozen=True)
class MetricDefinition:
    metric: str
    label: str
    build_version: str
    build_rows: BuildRowsFn


WOWY_METRIC = "wowy"


def _build_wowy_rows(
    *,
    scope_key: str,
    team_filter: str,
    season_type: str,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    combined_wowy_csv: Path,
    teams: list[str] | None,
) -> list[PlayerSeasonMetricRow]:
    records = prepare_wowy_player_season_records(
        teams=teams,
        seasons=None,
        season_type=season_type,
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
        combined_wowy_csv=combined_wowy_csv,
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


METRIC_DEFINITIONS = {
    WOWY_METRIC: MetricDefinition(
        metric=WOWY_METRIC,
        label="WOWY",
        build_version="wowy-player-season-v3",
        build_rows=_build_wowy_rows,
    )
}


def build_scope_key(
    *,
    teams: list[str] | None,
    season_type: str,
) -> tuple[str, str]:
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
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    combined_wowy_csv: Path,
    progress: RefreshProgressFn | None = None,
) -> None:
    definition = get_metric_definition(metric)
    source_fingerprint = build_cache_fingerprint(
        normalized_games_input_dir,
        normalized_game_players_input_dir,
    )
    cached_team_seasons = list_cached_team_seasons(normalized_games_input_dir)
    available_teams = sorted({team_season.team for team_season in cached_team_seasons})
    team_scopes: list[list[str] | None] = [None, *[[team] for team in available_teams]]

    for index, teams in enumerate(team_scopes):
        scope_key, team_filter = build_scope_key(teams=teams, season_type=season_type)
        scope_label = team_filter or "all-teams"
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
        if (
            metadata is not None
            and metadata.source_fingerprint == source_fingerprint
            and metadata.build_version == definition.build_version
        ):
            if progress is not None:
                progress(index + 1, len(team_scopes), f"cached {scope_label}")
            continue

        rows = definition.build_rows(
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            source_data_dir=source_data_dir,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            combined_wowy_csv=combined_wowy_csv,
            teams=teams,
        )
        replace_metric_rows(
            db_path,
            metric=definition.metric,
            scope_key=scope_key,
            metric_label=definition.label,
            build_version=definition.build_version,
            source_fingerprint=source_fingerprint,
            rows=rows,
        )
        _replace_metric_scope_rows(
            db_path=db_path,
            definition=definition,
            scope_key=scope_key,
            team_filter=team_filter,
            season_type=season_type,
            rows=rows,
            available_teams=available_teams,
            available_seasons=scope_seasons,
        )
        if progress is not None:
            progress(index + 1, len(team_scopes), f"built {scope_label}")


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
    catalog_row = load_metric_scope_catalog_row(db_path, metric, scope_key)
    if catalog_row is None:
        raise ValueError("Metric store has not been built for the requested scope")
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


def build_metric_options_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    teams: list[str] | None,
    season_type: str,
) -> dict[str, Any]:
    scope_key, _team_filter = build_scope_key(teams=teams, season_type=season_type)
    catalog_row = load_metric_scope_catalog_row(db_path, metric, scope_key)
    if catalog_row is None:
        raise ValueError("Metric store has not been built for the requested scope")
    return {
        "metric": catalog_row.metric,
        "metric_label": catalog_row.metric_label,
        "available_teams": catalog_row.available_teams,
        "available_seasons": catalog_row.available_seasons,
        "filters": {
            "team": sorted({team.upper() for team in teams or []}) or None,
            "season_type": catalog_row.season_type,
            "min_games_with": 15,
            "min_games_without": 2,
            "min_average_minutes": 30.0,
            "min_total_minutes": 600.0,
            "top_n": 30,
        },
    }


def build_metric_span_chart_payload(
    metric: str,
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    scope_key: str,
    top_n: int,
) -> dict[str, Any]:
    catalog_row = load_metric_scope_catalog_row(db_path, metric, scope_key)
    if catalog_row is None:
        raise ValueError("Metric store has not been built for the requested scope")
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
) -> None:
    span_series = build_metric_series(
        rows,
        seasons=available_seasons,
        top_n=len({row.player_id for row in rows}),
    )
    replace_metric_scope_catalog_row(
        db_path,
        row=MetricScopeCatalogRow(
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
    )
    replace_metric_full_span_rows(
        db_path,
        metric=definition.metric,
        scope_key=scope_key,
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


def get_metric_definition(metric: str) -> MetricDefinition:
    try:
        return METRIC_DEFINITIONS[metric]
    except KeyError as exc:
        raise ValueError(f"Unknown metric: {metric}") from exc


def build_cache_fingerprint(*directories: Path) -> str:
    digest = hashlib.sha256()
    for directory in directories:
        csv_paths = sorted(directory.glob("*.csv"))
        digest.update(str(directory).encode("utf-8"))
        for csv_path in csv_paths:
            stat = csv_path.stat()
            digest.update(csv_path.name.encode("utf-8"))
            digest.update(str(stat.st_size).encode("utf-8"))
            digest.update(str(stat.st_mtime_ns).encode("utf-8"))
    return digest.hexdigest()
