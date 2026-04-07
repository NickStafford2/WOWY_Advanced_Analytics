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
    build_rawr_query_from_request,
)


def register_rawr_routes(app: Flask) -> None:
    @app.get("/api/metrics/rawr/options")
    @web_route
    def get_rawr_options():
        return jsonify(build_rawr_options_payload(build_rawr_options_query_from_request(request)))

    @app.get("/api/metrics/rawr/player-seasons")
    @web_route
    def get_rawr_player_seasons():
        query = build_rawr_query_from_request(request)
        result = resolve_rawr_result(query)
        return jsonify(build_rawr_player_seasons_payload(query, result))

    @app.get("/api/metrics/rawr/span-chart")
    @web_route
    def get_rawr_span_chart():
        query = build_rawr_query_from_request(request)
        result = resolve_rawr_result(query)
        return jsonify(build_rawr_span_chart_payload(query, result))

    @app.get("/api/metrics/rawr/leaderboard")
    @app.get("/api/metrics/rawr/cached-leaderboard")
    @web_route
    def get_rawr_cached_leaderboard():
        return _rawr_json_leaderboard_response(recalculate=False)

    @app.get("/api/metrics/rawr/leaderboard.csv")
    @app.get("/api/metrics/rawr/cached-leaderboard.csv")
    @web_route
    def export_rawr_cached_leaderboard():
        return _rawr_csv_leaderboard_response(recalculate=False)

    @app.get("/api/metrics/rawr/custom-query")
    @web_route
    def get_rawr_custom_query():
        return _rawr_json_leaderboard_response(recalculate=True)

    @app.get("/api/metrics/rawr/custom-query.csv")
    @web_route
    def export_rawr_custom_query():
        return _rawr_csv_leaderboard_response(recalculate=True)


def _rawr_json_leaderboard_response(*, recalculate: bool = False) -> Response:
    query = build_rawr_query_from_request(request)
    result = resolve_rawr_result(query, recalculate=recalculate)
    return jsonify(build_rawr_leaderboard_payload(query, result, recalculate=recalculate))


def _rawr_csv_leaderboard_response(*, recalculate: bool = False) -> Response:
    query = build_rawr_query_from_request(request)
    result = resolve_rawr_result(query, recalculate=recalculate)
    filename = f"{Metric.RAWR.value}-all-players.csv"
    return Response(
        render_leaderboard_csv(
            metric=Metric.RAWR,
            table_rows=build_rawr_leaderboard_export(result),
        ),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
