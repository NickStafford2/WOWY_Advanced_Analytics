from __future__ import annotations

from flask import Flask, Response, jsonify, request

from rawr_analytics.app.rawr.service import (
    build_rawr_leaderboard_export,
    build_rawr_leaderboard_payload,
    build_rawr_options_payload,
    build_rawr_player_seasons_payload,
    build_rawr_span_chart_payload,
    resolve_rawr_result,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.web._csv import render_leaderboard_csv
from rawr_analytics.web._errors import web_route
from rawr_analytics.web._parse import (
    build_rawr_options_query_from_request,
    resolve_rawr_query_from_request,
)


def register_rawr_routes(app: Flask) -> None:
    @app.get("/api/metrics/rawr/options")
    @web_route
    def get_rawr_options():
        return json_options_response()

    @app.get("/api/metrics/rawr/player-seasons")
    @web_route
    def get_rawr_player_seasons():
        return rawr_json_player_seasons_response()

    @app.get("/api/metrics/rawr/span-chart")
    @web_route
    def get_rawr_span_chart():
        return rawr_json_span_chart_response()

    @app.get("/api/metrics/rawr/leaderboard")
    @app.get("/api/metrics/rawr/cached-leaderboard")
    @web_route
    def get_rawr_cached_leaderboard():
        return rawr_json_leaderboard_response(recalculate=False)

    @app.get("/api/metrics/rawr/leaderboard.csv")
    @app.get("/api/metrics/rawr/cached-leaderboard.csv")
    @web_route
    def export_rawr_cached_leaderboard():
        return rawr_csv_leaderboard_response(recalculate=False)

    @app.get("/api/metrics/rawr/custom-query")
    @web_route
    def get_rawr_custom_query():
        return rawr_json_leaderboard_response(recalculate=True)

    @app.get("/api/metrics/rawr/custom-query.csv")
    @web_route
    def export_rawr_custom_query():
        return rawr_csv_leaderboard_response(recalculate=True)


def json_options_response() -> Response:
    return jsonify(build_rawr_options_payload(build_rawr_options_query_from_request(request)))


def rawr_json_player_seasons_response() -> Response:
    query = resolve_rawr_query_from_request(request)
    result = resolve_rawr_result(query)
    return jsonify(build_rawr_player_seasons_payload(query, result))


def rawr_json_span_chart_response() -> Response:
    query = resolve_rawr_query_from_request(request)
    result = resolve_rawr_result(query)
    return jsonify(build_rawr_span_chart_payload(query, result))


def rawr_json_leaderboard_response(
    *,
    recalculate: bool = False,
) -> Response:
    query = resolve_rawr_query_from_request(request, recalculate=recalculate)
    result = resolve_rawr_result(query)
    return jsonify(build_rawr_leaderboard_payload(query, result))


def rawr_csv_leaderboard_response(
    *,
    recalculate: bool = False,
) -> Response:
    query = resolve_rawr_query_from_request(request, recalculate=recalculate)
    result = resolve_rawr_result(query)
    filename = f"{Metric.RAWR.value}-all-players.csv"
    return Response(
        render_leaderboard_csv(
            metric=Metric.RAWR,
            table_rows=build_rawr_leaderboard_export(result),
        ),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
