from __future__ import annotations

from pathlib import Path
from typing import Any

from wowy.apps.wowy.service import (
    available_wowy_seasons,
    build_wowy_span_chart_rows,
    prepare_wowy_player_season_records,
    serialize_wowy_player_season_records,
)
from wowy.nba.ingest import (
    DEFAULT_NORMALIZED_GAME_PLAYERS_DIR,
    DEFAULT_NORMALIZED_GAMES_DIR,
    DEFAULT_SOURCE_DATA_DIR,
    DEFAULT_WOWY_GAMES_DIR,
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
):
    from flask import Flask, jsonify, request

    app = Flask(__name__)

    @app.get("/api/wowy/player-seasons")
    def get_wowy_player_seasons():
        try:
            payload = _build_wowy_player_seasons_payload(
                request,
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


def _prepare_wowy_records_from_request(
    request,
    *,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    combined_wowy_csv: Path,
):
    return prepare_wowy_player_season_records(
        teams=request.args.getlist("team") or None,
        seasons=request.args.getlist("season") or None,
        season_type=request.args.get("season_type", "Regular Season"),
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
        combined_wowy_csv=combined_wowy_csv,
        min_games_with=_parse_optional_int(
            request.args.get("min_games_with"),
            default=15,
        ),
        min_games_without=_parse_optional_int(
            request.args.get("min_games_without"),
            default=2,
        ),
        min_average_minutes=_parse_optional_float(
            request.args.get("min_average_minutes"),
            default=30.0,
        ),
        min_total_minutes=_parse_optional_float(
            request.args.get("min_total_minutes"),
            default=600.0,
        ),
    )


def _build_wowy_player_seasons_payload(
    request,
    *,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    combined_wowy_csv: Path,
) -> dict[str, Any]:
    records = _prepare_wowy_records_from_request(
        request,
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
        combined_wowy_csv=combined_wowy_csv,
    )
    teams = request.args.getlist("team") or None
    seasons = request.args.getlist("season") or None
    season_type = request.args.get("season_type", "Regular Season")
    return {
        "metric": "wowy",
        "rows": serialize_wowy_player_season_records(records),
        "filters": _build_filters_payload(
            teams=teams,
            seasons=seasons,
            season_type=season_type,
            min_games_with=request.args.get("min_games_with"),
            min_games_without=request.args.get("min_games_without"),
            min_average_minutes=request.args.get("min_average_minutes"),
            min_total_minutes=request.args.get("min_total_minutes"),
        ),
    }


def _build_wowy_span_chart_payload(
    request,
    *,
    source_data_dir: Path,
    normalized_games_input_dir: Path,
    normalized_game_players_input_dir: Path,
    wowy_output_dir: Path,
    combined_wowy_csv: Path,
) -> dict[str, Any]:
    records = _prepare_wowy_records_from_request(
        request,
        source_data_dir=source_data_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        wowy_output_dir=wowy_output_dir,
        combined_wowy_csv=combined_wowy_csv,
    )
    seasons = available_wowy_seasons(records)
    if not seasons:
        raise ValueError("No player-season records matched the requested filters")

    start_season = request.args.get("start_season", seasons[0])
    end_season = request.args.get("end_season", seasons[-1])
    if start_season not in seasons:
        raise ValueError(f"Unknown start_season: {start_season}")
    if end_season not in seasons:
        raise ValueError(f"Unknown end_season: {end_season}")

    series = build_wowy_span_chart_rows(
        records,
        start_season=start_season,
        end_season=end_season,
        top_n=_parse_optional_int(request.args.get("top_n"), default=30),
    )
    return {
        "metric": "wowy",
        "metric_label": "WOWY",
        "span": {
            "start_season": start_season,
            "end_season": end_season,
            "available_seasons": seasons,
            "top_n": _parse_optional_int(request.args.get("top_n"), default=30),
        },
        "filters": _build_filters_payload(
            teams=request.args.getlist("team") or None,
            seasons=request.args.getlist("season") or None,
            season_type=request.args.get("season_type", "Regular Season"),
            min_games_with=request.args.get("min_games_with"),
            min_games_without=request.args.get("min_games_without"),
            min_average_minutes=request.args.get("min_average_minutes"),
            min_total_minutes=request.args.get("min_total_minutes"),
        ),
        "series": series,
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
