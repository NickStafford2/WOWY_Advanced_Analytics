from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from wowy.apps.wowy.models import WowyPlayerSeasonRecord
from wowy.apps.wowy.service import prepare_wowy_player_season_records
from wowy.data.player_metrics_db import (
    DEFAULT_PLAYER_METRICS_DB_PATH,
    PlayerSeasonMetricRow,
    list_metric_seasons,
    load_metric_rows,
    load_metric_store_metadata,
    replace_metric_rows,
)


WOWY_METRIC = "wowy"
WOWY_METRIC_LABEL = "WOWY"


def ensure_wowy_metric_store(
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    combined_wowy_csv: Path,
) -> None:
    source_fingerprint = build_cache_fingerprint(
        normalized_games_input_dir,
        normalized_game_players_input_dir,
    )
    metadata = load_metric_store_metadata(db_path, WOWY_METRIC)
    if metadata is not None and metadata.source_fingerprint == source_fingerprint:
        return

    records = prepare_wowy_player_season_records(
        teams=None,
        seasons=None,
        season_type="Regular Season",
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
    replace_metric_rows(
        db_path,
        metric=WOWY_METRIC,
        metric_label=WOWY_METRIC_LABEL,
        source_fingerprint=source_fingerprint,
        rows=[to_wowy_metric_row(record) for record in records],
    )


def build_wowy_player_seasons_payload(
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    seasons: list[str] | None,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    rows = load_metric_rows(
        db_path,
        metric=WOWY_METRIC,
        seasons=seasons,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return {
        "metric": WOWY_METRIC,
        "rows": [
            {
                "season": row.season,
                "player_id": row.player_id,
                "player_name": row.player_name,
                "games_with": row.games_with,
                "games_without": row.games_without,
                "avg_margin_with": row.details.get("avg_margin_with"),
                "avg_margin_without": row.details.get("avg_margin_without"),
                "wowy_score": row.value,
                "average_minutes": row.average_minutes,
                "total_minutes": row.total_minutes,
            }
            for row in rows
        ],
    }


def build_wowy_span_chart_payload(
    *,
    db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
    start_season: str | None,
    end_season: str | None,
    top_n: int,
    min_games_with: int,
    min_games_without: int,
    min_average_minutes: float | None,
    min_total_minutes: float | None,
) -> dict[str, Any]:
    available_seasons = list_metric_seasons(db_path, WOWY_METRIC)
    if not available_seasons:
        raise ValueError("No player-season records matched the requested filters")

    start_season = start_season or available_seasons[0]
    end_season = end_season or available_seasons[-1]
    if start_season not in available_seasons:
        raise ValueError(f"Unknown start_season: {start_season}")
    if end_season not in available_seasons:
        raise ValueError(f"Unknown end_season: {end_season}")
    if start_season > end_season:
        raise ValueError("start_season must be less than or equal to end_season")

    seasons = [
        season
        for season in available_seasons
        if start_season <= season <= end_season
    ]
    rows = load_metric_rows(
        db_path,
        metric=WOWY_METRIC,
        seasons=seasons,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )

    return {
        "metric": WOWY_METRIC,
        "metric_label": WOWY_METRIC_LABEL,
        "span": {
            "start_season": start_season,
            "end_season": end_season,
            "available_seasons": available_seasons,
            "top_n": top_n,
        },
        "series": build_metric_span_chart_rows(rows, seasons=seasons, top_n=top_n),
    }


def build_metric_span_chart_rows(
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


def to_wowy_metric_row(record: WowyPlayerSeasonRecord) -> PlayerSeasonMetricRow:
    return PlayerSeasonMetricRow(
        metric=WOWY_METRIC,
        metric_label=WOWY_METRIC_LABEL,
        season=record.season,
        player_id=record.player_id,
        player_name=record.player_name,
        value=record.wowy_score,
        games_with=record.games_with,
        games_without=record.games_without,
        average_minutes=record.average_minutes,
        total_minutes=record.total_minutes,
        details={
            "avg_margin_with": record.avg_margin_with,
            "avg_margin_without": record.avg_margin_without,
        },
    )


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
