from __future__ import annotations

from flask import Flask, Response, jsonify, request

from rawr_analytics.app.wowy.service import (
    build_wowy_leaderboard_export,
    build_wowy_leaderboard_payload,
    build_wowy_options_payload,
    build_wowy_player_seasons_payload,
    build_wowy_span_chart_payload,
    resolve_wowy_result,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.web._csv import render_leaderboard_csv
from rawr_analytics.web._errors import web_route
from rawr_analytics.web._parse import (
    build_wowy_options_query_from_request,
    build_wowy_query_from_request,
)


def register_wowy_routes(app: Flask) -> None:
    @app.get("/api/metrics/wowy/options")
    @web_route
    def get_wowy_options():
        return json_options_response(Metric.WOWY)

    @app.get("/api/metrics/wowy/player-seasons")
    @web_route
    def get_wowy_player_seasons():
        return json_player_seasons_response(Metric.WOWY)

    @app.get("/api/metrics/wowy/span-chart")
    @web_route
    def get_wowy_span_chart():
        return json_span_chart_response(Metric.WOWY)

    @app.get("/api/metrics/wowy/leaderboard")
    @app.get("/api/metrics/wowy/cached-leaderboard")
    @web_route
    def get_wowy_cached_leaderboard():
        return json_leaderboard_response(Metric.WOWY, recalculate=False)

    @app.get("/api/metrics/wowy/leaderboard.csv")
    @app.get("/api/metrics/wowy/cached-leaderboard.csv")
    @web_route
    def export_wowy_cached_leaderboard():
        return csv_leaderboard_response(Metric.WOWY, recalculate=False)

    @app.get("/api/metrics/wowy/custom-query")
    @web_route
    def get_wowy_custom_query():
        return json_leaderboard_response(Metric.WOWY, recalculate=True)

    @app.get("/api/metrics/wowy/custom-query.csv")
    @web_route
    def export_wowy_custom_query():
        return csv_leaderboard_response(Metric.WOWY, recalculate=True)


def json_options_response(metric: Metric) -> Response:
    return jsonify(
        build_wowy_options_payload(
            build_wowy_options_query_from_request(request),
            metric=metric,
        )
    )


def json_player_seasons_response(metric: Metric) -> Response:
    query = build_wowy_query_from_request(request)
    result = resolve_wowy_result(query, metric=metric)
    return jsonify(build_wowy_player_seasons_payload(query, result))


def json_span_chart_response(metric: Metric) -> Response:
    query = build_wowy_query_from_request(request)
    result = resolve_wowy_result(query, metric=metric)
    return jsonify(build_wowy_span_chart_payload(query, result))


def json_leaderboard_response(
    metric: Metric,
    *,
    recalculate: bool = False,
) -> Response:
    query = build_wowy_query_from_request(request)
    result = resolve_wowy_result(query, metric=metric, recalculate=recalculate)
    return jsonify(build_wowy_leaderboard_payload(query, result))


def csv_leaderboard_response(
    metric: Metric,
    *,
    recalculate: bool = False,
) -> Response:
    query = build_wowy_query_from_request(request)
    result = resolve_wowy_result(query, metric=metric, recalculate=recalculate)
    filename = f"{metric.value}-all-players.csv"
    return Response(
        render_leaderboard_csv(
            metric=metric,
            table_rows=build_wowy_leaderboard_export(query, result),
        ),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
