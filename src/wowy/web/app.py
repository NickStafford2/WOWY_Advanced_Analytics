from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypedDict

from wowy.apps.rawr.service import validate_filters as validate_rawr_filters
from wowy.apps.wowy.service import validate_filters as validate_wowy_filters
from wowy.data.player_metrics_db import DEFAULT_PLAYER_METRICS_DB_PATH
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.source.cache import DEFAULT_SOURCE_DATA_DIR
from wowy.web.metric_queries import (
    build_cached_metric_export_table_rows,
    build_cached_metric_leaderboard_payload,
    build_custom_metric_export_table_rows,
    build_custom_rawr_leaderboard_payload,
    build_custom_wowy_leaderboard_payload,
    build_custom_wowy_shrunk_leaderboard_payload,
    build_metric_filters_payload,
    build_metric_options_payload,
    build_metric_player_seasons_payload,
    build_metric_span_chart_payload,
)
from wowy.web.metric_store import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    RAWR_METRIC,
    WOWY_METRIC,
    WOWY_SHRUNK_METRIC,
    build_scope_key,
)


class ParsedRequestFilters(TypedDict):
    min_sample_size: int
    min_secondary_sample_size: int | None
    ridge_alpha: float | None
    min_average_minutes: float
    min_total_minutes: float
    top_n: int


def _parse_optional_int(raw_value: str | None, default: int) -> int:
    if raw_value is None:
        return default
    return int(raw_value)


def _parse_optional_float(raw_value: str | None, default: float) -> float:
    if raw_value is None:
        return default
    return float(raw_value)


def _parse_positive_int_list(raw_values: list[str]) -> list[int] | None:
    if not raw_values:
        return None
    parsed_values: list[int] = []
    for raw_value in raw_values:
        value = int(raw_value)
        if value <= 0:
            raise ValueError("team_id values must be positive integers")
        parsed_values.append(value)
    return parsed_values


def create_app(
    *,
    source_data_dir: Path = DEFAULT_SOURCE_DATA_DIR,
    player_metrics_db_path: Path = DEFAULT_PLAYER_METRICS_DB_PATH,
):
    from flask import Flask, Response, jsonify, request

    app = Flask(__name__)

    @app.get("/api/metrics/<metric>/options")
    def get_metric_options(metric: str):
        try:
            payload = build_metric_options_payload(
                metric,
                db_path=player_metrics_db_path,
                team_ids=_parse_positive_int_list(request.args.getlist("team_id")),
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

    @app.get("/api/metrics/<metric>/cached-leaderboard.csv")
    def export_metric_cached_leaderboard(metric: str):
        try:
            csv_content, filename = _build_cached_metric_leaderboard_csv(
                request,
                metric=metric,
                player_metrics_db_path=player_metrics_db_path,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return Response(
            csv_content,
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.get("/api/metrics/<metric>/custom-query")
    def get_metric_custom_query(metric: str):
        try:
            payload = _build_metric_custom_query_payload(
                request,
                metric=metric,
                source_data_dir=source_data_dir,
                player_metrics_db_path=player_metrics_db_path,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(payload)

    @app.get("/api/metrics/<metric>/custom-query.csv")
    def export_metric_custom_query(metric: str):
        try:
            csv_content, filename = _build_metric_custom_query_csv(
                request,
                metric=metric,
                source_data_dir=source_data_dir,
                player_metrics_db_path=player_metrics_db_path,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return Response(
            csv_content,
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

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

    @app.get("/api/wowy-shrunk/custom-query")
    def get_wowy_shrunk_custom_query():
        return get_metric_custom_query(WOWY_SHRUNK_METRIC)

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
    season_type = canonicalize_season_type(
        request.args.get("season_type", "Regular Season")
    )
    team_ids = _parse_positive_int_list(request.args.getlist("team_id"))
    seasons = _parse_request_seasons(request)
    scope_key, _team_filter = build_scope_key(team_ids=team_ids, season_type=season_type)
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
    payload["filters"] = build_metric_filters_payload(
        metric=metric,
        teams=None,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        min_sample_size=request.args.get("min_games_with")
        if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}
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
    season_type = canonicalize_season_type(
        request.args.get("season_type", "Regular Season")
    )
    team_ids = _parse_positive_int_list(request.args.getlist("team_id"))
    seasons = _parse_request_seasons(request)
    scope_key, _team_filter = build_scope_key(team_ids=team_ids, season_type=season_type)
    payload = build_metric_span_chart_payload(
        metric,
        db_path=player_metrics_db_path,
        scope_key=scope_key,
        top_n=filter_values["top_n"],
    )
    payload["filters"] = build_metric_filters_payload(
        metric=metric,
        teams=None,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        min_sample_size=request.args.get("min_games_with")
        if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}
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
    season_type = canonicalize_season_type(
        request.args.get("season_type", "Regular Season")
    )
    team_ids = _parse_positive_int_list(request.args.getlist("team_id"))
    seasons = _parse_request_seasons(request)
    scope_key, _team_filter = build_scope_key(team_ids=team_ids, season_type=season_type)
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
    payload["filters"] = build_metric_filters_payload(
        metric=metric,
        teams=None,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        min_sample_size=request.args.get("min_games_with")
        if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}
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
    player_metrics_db_path: Path,
) -> dict[str, Any]:
    filter_values = _parse_request_filters(
        request,
        metric=metric,
        include_top_n=True,
    )
    season_type = canonicalize_season_type(
        request.args.get("season_type", "Regular Season")
    )
    team_ids = _parse_positive_int_list(request.args.getlist("team_id"))
    seasons = _parse_request_seasons(request)
    if metric == WOWY_METRIC:
        min_games_without = filter_values["min_secondary_sample_size"]
        if min_games_without is None:
            raise ValueError("WOWY custom query requires min_games_without")
        payload = build_custom_wowy_leaderboard_payload(
            teams=None,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            top_n=filter_values["top_n"],
            source_data_dir=source_data_dir,
            player_metrics_db_path=player_metrics_db_path,
            min_games_with=int(filter_values["min_sample_size"]),
            min_games_without=min_games_without,
            min_average_minutes=filter_values["min_average_minutes"],
            min_total_minutes=filter_values["min_total_minutes"],
        )
    elif metric == WOWY_SHRUNK_METRIC:
        min_games_without = filter_values["min_secondary_sample_size"]
        if min_games_without is None:
            raise ValueError("WOWY shrunk custom query requires min_games_without")
        payload = build_custom_wowy_shrunk_leaderboard_payload(
            teams=None,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            top_n=filter_values["top_n"],
            source_data_dir=source_data_dir,
            player_metrics_db_path=player_metrics_db_path,
            min_games_with=int(filter_values["min_sample_size"]),
            min_games_without=min_games_without,
            min_average_minutes=filter_values["min_average_minutes"],
            min_total_minutes=filter_values["min_total_minutes"],
        )
    elif metric == RAWR_METRIC:
        ridge_alpha = filter_values["ridge_alpha"]
        if ridge_alpha is None:
            raise ValueError("RAWR custom query requires ridge_alpha")
        payload = build_custom_rawr_leaderboard_payload(
            teams=None,
            team_ids=team_ids,
            seasons=seasons,
            season_type=season_type,
            top_n=filter_values["top_n"],
            source_data_dir=source_data_dir,
            player_metrics_db_path=player_metrics_db_path,
            min_games=int(filter_values["min_sample_size"]),
            ridge_alpha=ridge_alpha,
            min_average_minutes=filter_values["min_average_minutes"],
            min_total_minutes=filter_values["min_total_minutes"],
        )
    else:
        raise ValueError(f"Unknown metric: {metric}")
    payload["filters"] = build_metric_filters_payload(
        metric=metric,
        teams=None,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        min_sample_size=request.args.get("min_games_with")
        if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}
        else request.args.get("min_games"),
        min_secondary_sample_size=request.args.get("min_games_without"),
        ridge_alpha=request.args.get("ridge_alpha"),
        min_average_minutes=request.args.get("min_average_minutes"),
        min_total_minutes=request.args.get("min_total_minutes"),
        top_n=request.args.get("top_n"),
    )
    return payload


def _build_cached_metric_leaderboard_csv(
    request,
    *,
    metric: str,
    player_metrics_db_path: Path,
) -> tuple[str, str]:
    filter_values = _parse_request_filters(
        request,
        metric=metric,
        include_top_n=True,
    )
    season_type = canonicalize_season_type(
        request.args.get("season_type", "Regular Season")
    )
    team_ids = _parse_positive_int_list(request.args.getlist("team_id"))
    seasons = _parse_request_seasons(request)
    scope_key, _team_filter = build_scope_key(team_ids=team_ids, season_type=season_type)
    metric_label, table_rows = build_cached_metric_export_table_rows(
        metric,
        db_path=player_metrics_db_path,
        scope_key=scope_key,
        seasons=seasons,
        min_average_minutes=filter_values["min_average_minutes"],
        min_total_minutes=filter_values["min_total_minutes"],
        min_sample_size=filter_values["min_sample_size"],
        min_secondary_sample_size=filter_values["min_secondary_sample_size"],
    )
    return _render_leaderboard_csv(metric_label=metric_label, table_rows=table_rows), (
        f"{metric}-all-players.csv"
    )


def _build_metric_custom_query_csv(
    request,
    *,
    metric: str,
    source_data_dir: Path,
    player_metrics_db_path: Path,
) -> tuple[str, str]:
    filter_values = _parse_request_filters(
        request,
        metric=metric,
        include_top_n=True,
    )
    season_type = canonicalize_season_type(
        request.args.get("season_type", "Regular Season")
    )
    team_ids = _parse_positive_int_list(request.args.getlist("team_id"))
    seasons = _parse_request_seasons(request)
    min_games_with = (
        int(filter_values["min_sample_size"])
        if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}
        else None
    )
    min_games_without = (
        filter_values["min_secondary_sample_size"]
        if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}
        else None
    )
    min_games = int(filter_values["min_sample_size"]) if metric == RAWR_METRIC else None
    ridge_alpha = filter_values["ridge_alpha"] if metric == RAWR_METRIC else None
    metric_label, table_rows = build_custom_metric_export_table_rows(
        metric,
        teams=None,
        team_ids=team_ids,
        seasons=seasons,
        season_type=season_type,
        source_data_dir=source_data_dir,
        player_metrics_db_path=player_metrics_db_path,
        min_games_with=min_games_with,
        min_games_without=min_games_without,
        min_games=min_games,
        ridge_alpha=ridge_alpha,
        min_average_minutes=filter_values["min_average_minutes"],
        min_total_minutes=filter_values["min_total_minutes"],
    )
    return _render_leaderboard_csv(metric_label=metric_label, table_rows=table_rows), (
        f"{metric}-all-players.csv"
    )


def _parse_request_filters(
    request,
    *,
    metric: str,
    include_top_n: bool,
) -> ParsedRequestFilters:
    ridge_alpha: float | None = None
    if metric in {WOWY_METRIC, WOWY_SHRUNK_METRIC}:
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


def _render_leaderboard_csv(
    *,
    metric_label: str,
    table_rows: list[dict[str, Any]],
) -> str:
    import csv
    from io import StringIO

    output = StringIO()
    writer = csv.writer(output)
    column_order = _build_csv_column_order(table_rows)
    writer.writerow(
        [_csv_header_label(column, metric_label=metric_label) for column in column_order]
    )
    for row in table_rows:
        writer.writerow([_format_csv_value(row.get(column)) for column in column_order])
    return output.getvalue()


def _build_csv_column_order(table_rows: list[dict[str, Any]]) -> list[str]:
    preferred_order = [
        "rank",
        "player_id",
        "player_name",
        "span_average_value",
        "average_minutes",
        "total_minutes",
        "games_with",
        "games_without",
        "avg_margin_with",
        "avg_margin_without",
        "season_count",
        "points",
    ]
    available_columns = {
        key
        for row in table_rows
        for key in row
    }
    ordered_columns = [column for column in preferred_order if column in available_columns]
    ordered_columns.extend(sorted(available_columns - set(ordered_columns)))
    return ordered_columns


def _csv_header_label(column: str, *, metric_label: str) -> str:
    return {
        "rank": "Rank",
        "player_id": "Player ID",
        "player_name": "Player",
        "span_average_value": metric_label,
        "average_minutes": "Avg Min",
        "total_minutes": "Tot Min",
        "games_with": "With",
        "games_without": "Without",
        "avg_margin_with": "Avg With",
        "avg_margin_without": "Avg Without",
        "season_count": "Seasons",
        "points": "Points",
    }.get(column, column)


def _format_csv_value(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, separators=(",", ":"))
    if isinstance(value, float):
        return f"{value:.6g}"
    if value is None:
        return "—"
    return str(value)
