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
    WOWY_METRIC,
    build_scope_key,
    build_metric_options_payload,
    build_metric_player_seasons_payload,
    build_metric_span_chart_payload,
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

    @app.get("/api/metrics/<metric>/options")
    def get_metric_options(metric: str):
        try:
            payload = build_metric_options_payload(
                metric,
                db_path=player_metrics_db_path,
                teams=request.args.getlist("team") or None,
                season_type=request.args.get("season_type", "Regular Season"),
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(payload)

    @app.get("/api/metrics/<metric>/player-seasons")
    def get_metric_player_seasons(metric: str):
        try:
            payload = _build_metric_player_seasons_payload(
                request,
                metric=metric,
                player_metrics_db_path=player_metrics_db_path,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(payload)

    @app.get("/api/metrics/<metric>/span-chart")
    def get_metric_span_chart(metric: str):
        try:
            payload = _build_metric_span_chart_payload(
                request,
                metric=metric,
                player_metrics_db_path=player_metrics_db_path,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(payload)

    @app.get("/api/wowy/player-seasons")
    def get_wowy_player_seasons():
        return get_metric_player_seasons(WOWY_METRIC)

    @app.get("/api/wowy/options")
    def get_wowy_options():
        return get_metric_options(WOWY_METRIC)

    @app.get("/api/wowy/span-chart")
    def get_wowy_span_chart():
        return get_metric_span_chart(WOWY_METRIC)

    return app


def _build_metric_player_seasons_payload(
    request,
    *,
    metric: str,
    player_metrics_db_path: Path,
) -> dict[str, Any]:
    filter_values = _parse_request_filters(request, include_top_n=False)
    season_type = request.args.get("season_type", "Regular Season")
    teams = request.args.getlist("team") or None
    scope_key, _team_filter = build_scope_key(teams=teams, season_type=season_type)
    payload = build_metric_player_seasons_payload(
        metric,
        db_path=player_metrics_db_path,
        scope_key=scope_key,
        seasons=request.args.getlist("season") or None,
        min_average_minutes=filter_values["min_average_minutes"],
        min_total_minutes=filter_values["min_total_minutes"],
        min_sample_size=filter_values["min_sample_size"],
        min_secondary_sample_size=filter_values["min_secondary_sample_size"],
    )
    payload["filters"] = _build_filters_payload(
        teams=teams,
        seasons=request.args.getlist("season") or None,
        season_type=season_type,
        min_games_with=request.args.get("min_games_with"),
        min_games_without=request.args.get("min_games_without"),
        min_average_minutes=request.args.get("min_average_minutes"),
        min_total_minutes=request.args.get("min_total_minutes"),
    )
    return payload


def _build_metric_span_chart_payload(
    request,
    *,
    metric: str,
    player_metrics_db_path: Path,
) -> dict[str, Any]:
    filter_values = _parse_request_filters(request, include_top_n=True)
    season_type = request.args.get("season_type", "Regular Season")
    teams = request.args.getlist("team") or None
    scope_key, _team_filter = build_scope_key(teams=teams, season_type=season_type)
    payload = build_metric_span_chart_payload(
        metric,
        db_path=player_metrics_db_path,
        scope_key=scope_key,
        top_n=filter_values["top_n"],
    )
    payload["filters"] = _build_filters_payload(
        teams=teams,
        seasons=request.args.getlist("season") or None,
        season_type=season_type,
        min_games_with=request.args.get("min_games_with"),
        min_games_without=request.args.get("min_games_without"),
        min_average_minutes=request.args.get("min_average_minutes"),
        min_total_minutes=request.args.get("min_total_minutes"),
    )
    return payload


def _parse_request_filters(
    request,
    *,
    include_top_n: bool,
) -> dict[str, int | float]:
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
        top_n=top_n if include_top_n else None,
        min_average_minutes=min_average_minutes,
        min_total_minutes=min_total_minutes,
    )
    return {
        "min_sample_size": min_games_with,
        "min_secondary_sample_size": min_games_without,
        "min_average_minutes": min_average_minutes,
        "min_total_minutes": min_total_minutes,
        "top_n": top_n,
    }


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
