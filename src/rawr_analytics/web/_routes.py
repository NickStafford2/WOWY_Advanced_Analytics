from __future__ import annotations

from flask import Flask

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.web._errors import web_route
from rawr_analytics.web._metric_handlers import MetricWebHandlers
from rawr_analytics.web._metric_registry import get_metric_handlers


def _get_metric_handlers(metric: str) -> MetricWebHandlers:
    return get_metric_handlers(Metric.parse(metric))


def register_metric_routes(app: Flask) -> None:
    @app.get("/api/metrics/<metric>/options")
    @web_route
    def get_metric_options(metric: str):
        return _get_metric_handlers(metric).json_options_response()

    @app.get("/api/metrics/<metric>/player-seasons")
    @web_route
    def get_metric_player_seasons(metric: str):
        return _get_metric_handlers(metric).json_player_seasons_response()

    @app.get("/api/metrics/<metric>/span-chart")
    @web_route
    def get_metric_span_chart(metric: str):
        return _get_metric_handlers(metric).json_span_chart_response()

    @app.get("/api/metrics/<metric>/leaderboard")
    @app.get("/api/metrics/<metric>/cached-leaderboard")
    @web_route
    def get_metric_cached_leaderboard(metric: str):
        return _get_metric_handlers(metric).json_leaderboard_response(False)

    @app.get("/api/metrics/<metric>/leaderboard.csv")
    @app.get("/api/metrics/<metric>/cached-leaderboard.csv")
    @web_route
    def export_metric_cached_leaderboard(metric: str):
        return _get_metric_handlers(metric).csv_leaderboard_response(False)

    @app.get("/api/metrics/<metric>/custom-query")
    @web_route
    def get_metric_custom_query(metric: str):
        return _get_metric_handlers(metric).json_leaderboard_response(True)

    @app.get("/api/metrics/<metric>/custom-query.csv")
    @web_route
    def export_metric_custom_query(metric: str):
        return _get_metric_handlers(metric).csv_leaderboard_response(True)
