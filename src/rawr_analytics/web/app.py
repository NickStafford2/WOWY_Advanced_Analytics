from __future__ import annotations

import json
from typing import Any

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.frontend import (
    MetricQuery,
    build_metric_options_payload,
    build_metric_query,
    build_metric_view_payload,
)


def _parse_optional_int(raw_value: str | None) -> int | None:
    return None if raw_value is None else int(raw_value)


def _parse_optional_float(raw_value: str | None) -> float | None:
    return None if raw_value is None else float(raw_value)


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


def create_app():
    from flask import Flask, Response, jsonify, request

    app = Flask(__name__)

    def parse_metric_query(metric: str) -> MetricQuery:
        metric_type = Metric.parse(metric)
        return build_metric_query(
            metric_type,
            season_type=request.args.get("season_type", "Regular Season"),
            team_ids=_parse_positive_int_list(request.args.getlist("team_id")),
            seasons=request.args.getlist("season") or None,
            top_n=_parse_optional_int(request.args.get("top_n")),
            min_average_minutes=_parse_optional_float(request.args.get("min_average_minutes")),
            min_total_minutes=_parse_optional_float(request.args.get("min_total_minutes")),
            min_games=_parse_optional_int(request.args.get("min_games")),
            ridge_alpha=_parse_optional_float(request.args.get("ridge_alpha")),
            min_games_with=_parse_optional_int(request.args.get("min_games_with")),
            min_games_without=_parse_optional_int(request.args.get("min_games_without")),
        )

    def json_metric_response(metric: str, view: str):
        metric_type = Metric.parse(metric)
        query = parse_metric_query(metric)
        payload = build_metric_view_payload(
            metric_type,
            view=view,
            query=query,
        )
        return jsonify(payload)

    def run_json(handler):
        try:
            return handler()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    @app.get("/api/metrics/<metric>/options")
    def get_metric_options(metric: str):
        return run_json(
            lambda: jsonify(
                build_metric_options_payload(
                    Metric.parse(metric),
                    team_ids=_parse_positive_int_list(request.args.getlist("team_id")),
                    season_type=request.args.get("season_type", "Regular Season"),
                )
            )
        )

    @app.get("/api/metrics/<metric>/player-seasons")
    def get_metric_player_seasons(metric: str):
        return run_json(lambda: json_metric_response(metric, "player-seasons"))

    @app.get("/api/metrics/<metric>/span-chart")
    def get_metric_span_chart(metric: str):
        return run_json(lambda: json_metric_response(metric, "span-chart"))

    @app.get("/api/metrics/<metric>/cached-leaderboard")
    def get_metric_cached_leaderboard(metric: str):
        return run_json(lambda: json_metric_response(metric, "cached-leaderboard"))

    @app.get("/api/metrics/<metric>/cached-leaderboard.csv")
    def export_metric_cached_leaderboard(metric: str):
        return run_json(lambda: csv_metric_response(metric, "cached-leaderboard"))

    @app.get("/api/metrics/<metric>/custom-query")
    def get_metric_custom_query(metric: str):
        return run_json(lambda: json_metric_response(metric, "custom-query"))

    @app.get("/api/metrics/<metric>/custom-query.csv")
    def export_metric_custom_query(metric: str):
        return run_json(lambda: csv_metric_response(metric, "custom-query"))

    @app.get("/api/wowy/player-seasons")
    def get_wowy_player_seasons():
        return get_metric_player_seasons(Metric.WOWY.value)

    @app.get("/api/wowy/options")
    def get_wowy_options():
        return get_metric_options(Metric.WOWY.value)

    @app.get("/api/wowy/span-chart")
    def get_wowy_span_chart():
        return get_metric_span_chart(Metric.WOWY.value)

    @app.get("/api/wowy/cached-leaderboard")
    def get_wowy_cached_leaderboard():
        return get_metric_cached_leaderboard(Metric.WOWY.value)

    @app.get("/api/wowy/custom-query")
    def get_wowy_custom_query():
        return get_metric_custom_query(Metric.WOWY.value)

    @app.get("/api/wowy-shrunk/custom-query")
    def get_wowy_shrunk_custom_query():
        return get_metric_custom_query(Metric.WOWY_SHRUNK.value)

    @app.get("/api/rawr/custom-query")
    def get_rawr_custom_query():
        return get_metric_custom_query(Metric.RAWR.value)

    return app


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
    available_columns = {key for row in table_rows for key in row}
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
