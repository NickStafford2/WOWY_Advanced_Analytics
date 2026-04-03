from __future__ import annotations

import json
from typing import Any

from rawr_analytics.services.metric_query import (
    build_metric_options_payload,
    build_metric_options_request,
    build_metric_query_export,
    build_metric_query_request,
    build_metric_query_view,
    serialize_service_value,
)


def create_app():
    from flask import Flask, Response, jsonify, request

    app = Flask(__name__)

    def parse_metric_query(metric: str):
        return build_metric_query_request(
            metric=metric,
            get_arg=request.args.get,
            get_list=request.args.getlist,
        )

    def json_metric_response(metric: str, view: str):
        result = build_metric_query_view(
            parse_metric_query(metric),
            view=view,
        )
        return jsonify(serialize_service_value(result.payload))

    def run_json(handler):
        try:
            return handler()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

    def csv_metric_response(metric: str, view: str):
        result = build_metric_query_export(
            parse_metric_query(metric),
            view=view,
        )
        filename = f"{metric}-all-players.csv"
        return Response(
            _render_leaderboard_csv(metric_label=result.metric_label, table_rows=result.rows),
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.get("/api/metrics/<metric>/options")
    def get_metric_options(metric: str):
        return run_json(
            lambda: jsonify(
                serialize_service_value(
                    build_metric_options_payload(
                        build_metric_options_request(
                            metric=metric,
                            get_arg=request.args.get,
                            get_list=request.args.getlist,
                        )
                    )
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
        return get_metric_player_seasons("wowy")

    @app.get("/api/wowy/options")
    def get_wowy_options():
        return get_metric_options("wowy")

    @app.get("/api/wowy/span-chart")
    def get_wowy_span_chart():
        return get_metric_span_chart("wowy")

    @app.get("/api/wowy/cached-leaderboard")
    def get_wowy_cached_leaderboard():
        return get_metric_cached_leaderboard("wowy")

    @app.get("/api/wowy/custom-query")
    def get_wowy_custom_query():
        return get_metric_custom_query("wowy")

    @app.get("/api/wowy-shrunk/custom-query")
    def get_wowy_shrunk_custom_query():
        return get_metric_custom_query("wowy-shrunk")

    @app.get("/api/rawr/custom-query")
    def get_rawr_custom_query():
        return get_metric_custom_query("rawr")

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
