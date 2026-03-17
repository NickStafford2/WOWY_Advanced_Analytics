from __future__ import annotations

from pathlib import Path
from typing import Any

from wowy.apps.rawr.service import validate_filters as validate_rawr_filters
from wowy.apps.wowy.service import validate_filters as validate_wowy_filters
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
)
from wowy.nba.seasons import canonicalize_season_string
from wowy.web.service import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    RAWR_METRIC,
    WOWY_METRIC,
    build_custom_rawr_leaderboard_payload,
    build_metric_default_filters_payload,
    build_scope_key,
    build_cached_metric_leaderboard_payload,
    build_custom_wowy_leaderboard_payload,
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

    @app.get("/api/metrics/<metric>/cached-leaderboard")
    def get_metric_cached_leaderboard(metric: str):
        try:
            payload = _build_cached_metric_leaderboard_payload(
                request,
                metric=metric,
                player_metrics_db_path=player_metrics_db_path,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(payload)

    @app.get("/api/metrics/<metric>/custom-query")
    def get_metric_custom_query(metric: str):
        try:
            payload = _build_metric_custom_query_payload(
                request,
                metric=metric,
                source_data_dir=source_data_dir,
                normalized_games_input_dir=normalized_games_input_dir,
                normalized_game_players_input_dir=normalized_game_players_input_dir,
                wowy_output_dir=wowy_output_dir,
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

    @app.get("/api/wowy/cached-leaderboard")
    def get_wowy_cached_leaderboard():
        return get_metric_cached_leaderboard(WOWY_METRIC)

    @app.get("/api/wowy/custom-query")
    def get_wowy_custom_query():
        return get_metric_custom_query(WOWY_METRIC)

    @app.get("/api/rawr/custom-query")
    def get_rawr_custom_query():
        return get_metric_custom_query(RAWR_METRIC)

    return app


def _build_metric_player_seasons_payload(
    request,
    *,
    metric: str,
    player_metrics_db_path: Path,
) -> dict[str, Any]:
    filter_values = _parse_request_filters(
        request,
        metric=metric,
        include_top_n=False,
    )
    season_type = request.args.get("season_type", "Regular Season")
    teams = request.args.getlist("team") or None
    seasons = _parse_request_seasons(request)
    scope_key, _team_filter = build_scope_key(teams=teams, season_type=season_type)
    payload = build_metric_player_seasons_payload(
        metric,
        db_path=player_metrics_db_path,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=filter_values["min_average_minutes"],
        min_total_minutes=filter_values["min_total_minutes"],
        min_sample_size=filter_values["min_sample_size"],
        min_secondary_sample_size=filter_values["min_secondary_sample_size"],
    )
    payload["filters"] = _build_filters_payload(
        metric=metric,
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        min_sample_size=request.args.get("min_games_with")
        if metric == WOWY_METRIC
        else request.args.get("min_games"),
        min_secondary_sample_size=request.args.get("min_games_without"),
        ridge_alpha=request.args.get("ridge_alpha"),
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
    filter_values = _parse_request_filters(
        request,
        metric=metric,
        include_top_n=True,
    )
    season_type = request.args.get("season_type", "Regular Season")
    teams = request.args.getlist("team") or None
    seasons = _parse_request_seasons(request)
    scope_key, _team_filter = build_scope_key(teams=teams, season_type=season_type)
    payload = build_metric_span_chart_payload(
        metric,
        db_path=player_metrics_db_path,
        scope_key=scope_key,
        top_n=filter_values["top_n"],
    )
    payload["filters"] = _build_filters_payload(
        metric=metric,
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        min_sample_size=request.args.get("min_games_with")
        if metric == WOWY_METRIC
        else request.args.get("min_games"),
        min_secondary_sample_size=request.args.get("min_games_without"),
        ridge_alpha=request.args.get("ridge_alpha"),
        min_average_minutes=request.args.get("min_average_minutes"),
        min_total_minutes=request.args.get("min_total_minutes"),
        top_n=request.args.get("top_n"),
    )
    return payload


def _build_cached_metric_leaderboard_payload(
    request,
    *,
    metric: str,
    player_metrics_db_path: Path,
) -> dict[str, Any]:
    filter_values = _parse_request_filters(
        request,
        metric=metric,
        include_top_n=True,
    )
    season_type = request.args.get("season_type", "Regular Season")
    teams = request.args.getlist("team") or None
    seasons = _parse_request_seasons(request)
    scope_key, _team_filter = build_scope_key(teams=teams, season_type=season_type)
    payload = build_cached_metric_leaderboard_payload(
        metric,
        db_path=player_metrics_db_path,
        scope_key=scope_key,
        top_n=filter_values["top_n"],
        seasons=seasons,
        min_average_minutes=filter_values["min_average_minutes"],
        min_total_minutes=filter_values["min_total_minutes"],
        min_sample_size=filter_values["min_sample_size"],
        min_secondary_sample_size=filter_values["min_secondary_sample_size"],
    )
    payload["filters"] = _build_filters_payload(
        metric=metric,
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        min_sample_size=request.args.get("min_games_with")
        if metric == WOWY_METRIC
        else request.args.get("min_games"),
        min_secondary_sample_size=request.args.get("min_games_without"),
        ridge_alpha=request.args.get("ridge_alpha"),
        min_average_minutes=request.args.get("min_average_minutes"),
        min_total_minutes=request.args.get("min_total_minutes"),
        top_n=request.args.get("top_n"),
    )
    return payload


def _build_metric_custom_query_payload(
    request,
    *,
    metric: str,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    player_metrics_db_path: Path,
) -> dict[str, Any]:
    filter_values = _parse_request_filters(
        request,
        metric=metric,
        include_top_n=True,
    )
    season_type = request.args.get("season_type", "Regular Season")
    teams = request.args.getlist("team") or None
    seasons = _parse_request_seasons(request)
    if metric == WOWY_METRIC:
        payload = build_custom_wowy_leaderboard_payload(
            teams=teams,
            seasons=seasons,
            season_type=season_type,
            top_n=filter_values["top_n"],
            source_data_dir=source_data_dir,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            player_metrics_db_path=player_metrics_db_path,
            min_games_with=int(filter_values["min_sample_size"]),
            min_games_without=int(filter_values["min_secondary_sample_size"]),
            min_average_minutes=float(filter_values["min_average_minutes"]),
            min_total_minutes=float(filter_values["min_total_minutes"]),
        )
    elif metric == RAWR_METRIC:
        payload = build_custom_rawr_leaderboard_payload(
            teams=teams,
            seasons=seasons,
            season_type=season_type,
            top_n=filter_values["top_n"],
            source_data_dir=source_data_dir,
            normalized_games_input_dir=normalized_games_input_dir,
            normalized_game_players_input_dir=normalized_game_players_input_dir,
            wowy_output_dir=wowy_output_dir,
            player_metrics_db_path=player_metrics_db_path,
            min_games=int(filter_values["min_sample_size"]),
            ridge_alpha=float(filter_values["ridge_alpha"]),
            min_average_minutes=float(filter_values["min_average_minutes"]),
            min_total_minutes=float(filter_values["min_total_minutes"]),
        )
    else:
        raise ValueError(f"Unknown metric: {metric}")
    payload["filters"] = _build_filters_payload(
        metric=metric,
        teams=teams,
        seasons=seasons,
        season_type=season_type,
        min_sample_size=request.args.get("min_games_with")
        if metric == WOWY_METRIC
        else request.args.get("min_games"),
        min_secondary_sample_size=request.args.get("min_games_without"),
        ridge_alpha=request.args.get("ridge_alpha"),
        min_average_minutes=request.args.get("min_average_minutes"),
        min_total_minutes=request.args.get("min_total_minutes"),
        top_n=request.args.get("top_n"),
    )
    return payload


def _parse_request_filters(
    request,
    *,
    metric: str,
    include_top_n: bool,
) -> dict[str, int | float]:
    if metric == WOWY_METRIC:
        min_sample_size = _parse_optional_int(
            request.args.get("min_games_with"),
            default=15,
        )
        min_secondary_sample_size = _parse_optional_int(
            request.args.get("min_games_without"),
            default=2,
        )
        validate_wowy_filters(
            min_sample_size,
            min_secondary_sample_size,
            top_n=_parse_optional_int(request.args.get("top_n"), default=30)
            if include_top_n
            else None,
            min_average_minutes=_parse_optional_float(
                request.args.get("min_average_minutes"),
                default=30.0,
            ),
            min_total_minutes=_parse_optional_float(
                request.args.get("min_total_minutes"),
                default=600.0,
            ),
        )
    elif metric == RAWR_METRIC:
        min_sample_size = _parse_optional_int(
            request.args.get("min_games"),
            default=35,
        )
        min_secondary_sample_size = None
        ridge_alpha = _parse_optional_float(
            request.args.get("ridge_alpha"),
            default=DEFAULT_RAWR_RIDGE_ALPHA,
        )
        validate_rawr_filters(
            min_sample_size,
            ridge_alpha=ridge_alpha,
            top_n=_parse_optional_int(request.args.get("top_n"), default=30)
            if include_top_n
            else None,
            min_average_minutes=_parse_optional_float(
                request.args.get("min_average_minutes"),
                default=30.0,
            ),
            min_total_minutes=_parse_optional_float(
                request.args.get("min_total_minutes"),
                default=600.0,
            ),
        )
    else:
        raise ValueError(f"Unknown metric: {metric}")

    min_average_minutes = _parse_optional_float(
        request.args.get("min_average_minutes"),
        default=30.0,
    )
    min_total_minutes = _parse_optional_float(
        request.args.get("min_total_minutes"),
        default=600.0,
    )
    top_n = _parse_optional_int(request.args.get("top_n"), default=30)
    return {
        "min_sample_size": min_sample_size,
        "min_secondary_sample_size": min_secondary_sample_size,
        "ridge_alpha": ridge_alpha if metric == RAWR_METRIC else None,
        "min_average_minutes": min_average_minutes,
        "min_total_minutes": min_total_minutes,
        "top_n": top_n,
    }


def _parse_request_seasons(request) -> list[str] | None:
    raw_seasons = request.args.getlist("season")
    if not raw_seasons:
        return None
    return [canonicalize_season_string(season) for season in raw_seasons]


def _build_filters_payload(
    *,
    metric: str,
    teams: list[str] | None,
    seasons: list[str] | None,
    season_type: str,
    min_sample_size: str | None,
    min_secondary_sample_size: str | None,
    ridge_alpha: str | None,
    min_average_minutes: str | None,
    min_total_minutes: str | None,
    top_n: str | None = None,
) -> dict[str, Any]:
    defaults = build_metric_default_filters_payload(
        metric,
        teams=teams,
        season_type=season_type,
    )
    payload = {
        "team": teams,
        "season": seasons,
        "season_type": season_type,
        "min_average_minutes": _parse_optional_float(
            min_average_minutes,
            default=30.0,
        ),
        "min_total_minutes": _parse_optional_float(
            min_total_minutes,
            default=600.0,
        ),
        "top_n": _parse_optional_int(top_n, default=30),
    }
    if metric == WOWY_METRIC:
        payload["min_games_with"] = _parse_optional_int(
            min_sample_size,
            default=int(defaults["min_games_with"]),
        )
        payload["min_games_without"] = _parse_optional_int(
            min_secondary_sample_size,
            default=int(defaults["min_games_without"]),
        )
        return payload
    if metric == RAWR_METRIC:
        payload["min_games"] = _parse_optional_int(
            min_sample_size,
            default=int(defaults["min_games"]),
        )
        payload["ridge_alpha"] = _parse_optional_float(
            ridge_alpha,
            default=float(defaults["ridge_alpha"]),
        )
        return payload
    raise ValueError(f"Unknown metric: {metric}")
