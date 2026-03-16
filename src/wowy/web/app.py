from __future__ import annotations

from pathlib import Path
from typing import Any

from wowy.apps.wowy.service import validate_filters
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)
from wowy.web.service import (
    build_wowy_player_seasons_payload,
    build_wowy_span_chart_payload,
    ensure_wowy_metric_store,
)


def _parse_optional_int(raw_value: str | None, default: int) -> int:
    if raw_value is None:
        return default
    return int(raw_value)


def _parse_optional_float(raw_value: str | None, default: float) -> float:
    if raw_value is None:
        return default
    return float(raw_value)


def create_app(
    *,
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    normalized_games_input_dir: Path = DEFAULT_NORMALIZED_GAMES_DIR,
    normalized_game_players_input_dir: Path = DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    wowy_output_dir: Path = DEFAULT_WOWY_GAMES_DIR,
    combined_wowy_csv: Path = Path("data/combined/wowy/games.csv"),
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
):
    from flask import Flask, jsonify, request

    app = Flask(__name__)

    @app.get("/api/wowy/player-seasons")
    def get_wowy_player_seasons():
        try:
            payload = _build_wowy_player_seasons_payload(
                request,
                player_metrics_db_path=player_metrics_db_path,
                source_data_dir=source_data_dir,
                normalized_games_input_dir=normalized_games_input_dir,
                normalized_game_players_input_dir=normalized_game_players_input_dir,
                wowy_output_dir=wowy_output_dir,
                combined_wowy_csv=combined_wowy_csv,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(payload)

    @app.get("/api/wowy/span-chart")
    def get_wowy_span_chart():
        try:
            payload = _build_wowy_span_chart_payload(
                request,
                player_metrics_db_path=player_metrics_db_path,
                source_data_dir=source_data_dir,
                normalized_games_input_dir=normalized_games_input_dir,
                normalized_game_players_input_dir=normalized_game_players_input_dir,
                wowy_output_dir=wowy_output_dir,
                combined_wowy_csv=combined_wowy_csv,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(payload)

    return app


def _build_wowy_player_seasons_payload(
    request,
    *,
    player_metrics_db_path: Path,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    combined_wowy_csv: Path,
) -> dict[str, Any]:
    min_games_with = _parse_optional_int(
        request.args.get("min_games_with"),
        default=15,
    )
    min_games_without = _parse_optional_int(
        request.args.get("min_games_without"),
        default=2,
    )
    min_average_minutes = _parse_optional_float(
        request.args.get("min_average_minutes"),
        default=30.0,
    )
    min_total_minutes = _parse_optional_float(
        request.args.get("min_total_minutes"),
        default=600.0,
    )
    validate_filters(
        min_games_with,
        min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    ensure_wowy_metric_store(
        db_path=player_metrics_db_path,
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
        combined_wowy_csv=combined_wowy_csv,
    )
    teams = request.args.getlist("team") or None
    seasons = request.args.getlist("season") or None
    season_type = request.args.get("season_type", "Regular Season")
    payload = build_wowy_player_seasons_payload(
        db_path=player_metrics_db_path,
        seasons=seasons,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    payload["filters"] = _build_filters_payload(
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        min_games_with=request.args.get("min_games_with"),
        min_games_without=request.args.get("min_games_without"),
        min_average_minutes=request.args.get("min_average_minutes"),
        min_total_minutes=request.args.get("min_total_minutes"),
    )
    return payload


def _build_wowy_span_chart_payload(
    request,
    *,
    player_metrics_db_path: Path,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    combined_wowy_csv: Path,
) -> dict[str, Any]:
    min_games_with = _parse_optional_int(
        request.args.get("min_games_with"),
        default=15,
    )
    min_games_without = _parse_optional_int(
        request.args.get("min_games_without"),
        default=2,
    )
    min_average_minutes = _parse_optional_float(
        request.args.get("min_average_minutes"),
        default=30.0,
    )
    min_total_minutes = _parse_optional_float(
        request.args.get("min_total_minutes"),
        default=600.0,
    )
    top_n = _parse_optional_int(request.args.get("top_n"), default=30)
    validate_filters(
        min_games_with,
        min_games_without,
        top_n=top_n,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    ensure_wowy_metric_store(
        db_path=player_metrics_db_path,
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
        combined_wowy_csv=combined_wowy_csv,
    )
    payload = build_wowy_span_chart_payload(
        db_path=player_metrics_db_path,
        start_season=request.args.get("start_season"),
        end_season=request.args.get("end_season"),
        top_n=top_n,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    payload["filters"] = _build_filters_payload(
        teams=request.args.getlist("team") or None,
        seasons=request.args.getlist("season") or None,
        season_type=request.args.get("season_type", "Regular Season"),
        min_games_with=request.args.get("min_games_with"),
        min_games_without=request.args.get("min_games_without"),
        min_average_minutes=request.args.get("min_average_minutes"),
        min_total_minutes=request.args.get("min_total_minutes"),
    )
    return payload


def _build_filters_payload(
    *,
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str,
    min_games_with: str | None,
    min_games_without: str | None,
    min_average_minutes: str | None,
    min_total_minutes: str | None,
) -> dict[str, Any]:
    return {
        "team": teams,
        "season": seasons,
        "season_type": season_type,
        "min_games_with": _parse_optional_int(min_games_with, default=15),
        "min_games_without": _parse_optional_int(min_games_without, default=2),
        "min_average_minutes": _parse_optional_float(
            min_average_minutes,
            default=30.0,
        ),
        "min_total_minutes": _parse_optional_float(
            min_total_minutes,
            default=600.0,
        ),
    }
