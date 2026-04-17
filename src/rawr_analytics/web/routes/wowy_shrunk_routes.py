from __future__ import annotations

from flask import Flask

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.web._errors import web_route
from rawr_analytics.web.routes.wowy_routes import (
    csv_leaderboard_response,
    json_leaderboard_response,
    json_options_response,
    json_player_seasons_response,
    json_span_chart_response,
    sse_leaderboard_response,
)


def register_wowy_shrunk_routes(app: Flask) -> None:
    @app.get("/api/metrics/wowy_shrunk/options")
    @web_route
    def get_wowy_shrunk_options():
        return json_options_response(Metric.WOWY_SHRUNK)

    @app.get("/api/metrics/wowy_shrunk/player-seasons")
    @web_route
    def get_wowy_shrunk_player_seasons():
        return json_player_seasons_response(Metric.WOWY_SHRUNK)

    @app.get("/api/metrics/wowy_shrunk/span-chart")
    @web_route
    def get_wowy_shrunk_span_chart():
        return json_span_chart_response(Metric.WOWY_SHRUNK)

    @app.get("/api/metrics/wowy_shrunk/leaderboard")
    @app.get("/api/metrics/wowy_shrunk/cached-leaderboard")
    @web_route
    def get_wowy_shrunk_cached_leaderboard():
        return json_leaderboard_response(Metric.WOWY_SHRUNK, recalculate=False)

    @app.get("/api/metrics/wowy_shrunk/leaderboard/stream")
    @app.get("/api/metrics/wowy_shrunk/cached-leaderboard/stream")
    def get_wowy_shrunk_cached_leaderboard_stream():
        return sse_leaderboard_response(Metric.WOWY_SHRUNK, recalculate=False)

    @app.get("/api/metrics/wowy_shrunk/leaderboard.csv")
    @app.get("/api/metrics/wowy_shrunk/cached-leaderboard.csv")
    @web_route
    def export_wowy_shrunk_cached_leaderboard():
        return csv_leaderboard_response(Metric.WOWY_SHRUNK, recalculate=False)

    @app.get("/api/metrics/wowy_shrunk/custom-query")
    @web_route
    def get_wowy_shrunk_custom_query():
        return json_leaderboard_response(Metric.WOWY_SHRUNK, recalculate=True)

    @app.get("/api/metrics/wowy_shrunk/custom-query/stream")
    def get_wowy_shrunk_custom_query_stream():
        return sse_leaderboard_response(Metric.WOWY_SHRUNK, recalculate=True)

    @app.get("/api/metrics/wowy_shrunk/custom-query.csv")
    @web_route
    def export_wowy_shrunk_custom_query():
        return csv_leaderboard_response(Metric.WOWY_SHRUNK, recalculate=True)
