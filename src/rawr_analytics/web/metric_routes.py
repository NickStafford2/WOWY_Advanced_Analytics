from __future__ import annotations

from flask import Flask

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.web._errors import web_route
from rawr_analytics.web.rawr_routes import (
    csv_leaderboard_response as csv_rawr_leaderboard_response,
)
from rawr_analytics.web.rawr_routes import (
    json_leaderboard_response as json_rawr_leaderboard_response,
)
from rawr_analytics.web.rawr_routes import (
    json_options_response as json_rawr_options_response,
)
from rawr_analytics.web.rawr_routes import (
    json_player_seasons_response as json_rawr_player_seasons_response,
)
from rawr_analytics.web.rawr_routes import (
    json_span_chart_response as json_rawr_span_chart_response,
)
from rawr_analytics.web.wowy_routes import (
    csv_leaderboard_response as csv_wowy_leaderboard_response,
)
from rawr_analytics.web.wowy_routes import (
    json_leaderboard_response as json_wowy_leaderboard_response,
)
from rawr_analytics.web.wowy_routes import (
    json_options_response as json_wowy_options_response,
)
from rawr_analytics.web.wowy_routes import (
    json_player_seasons_response as json_wowy_player_seasons_response,
)
from rawr_analytics.web.wowy_routes import (
    json_span_chart_response as json_wowy_span_chart_response,
)


def _json_options_response(parsed_metric: Metric):
    match parsed_metric:
        case Metric.RAWR:
            return json_rawr_options_response()
        case Metric.WOWY | Metric.WOWY_SHRUNK:
            return json_wowy_options_response(parsed_metric)
        case _:
            raise ValueError(f"Unsupported web metric: {parsed_metric}")


def _json_player_seasons_response(parsed_metric: Metric):
    match parsed_metric:
        case Metric.RAWR:
            return json_rawr_player_seasons_response()
        case Metric.WOWY | Metric.WOWY_SHRUNK:
            return json_wowy_player_seasons_response(parsed_metric)
        case _:
            raise ValueError(f"Unsupported web metric: {parsed_metric}")


def _json_span_chart_response(parsed_metric: Metric):
    match parsed_metric:
        case Metric.RAWR:
            return json_rawr_span_chart_response()
        case Metric.WOWY | Metric.WOWY_SHRUNK:
            return json_wowy_span_chart_response(parsed_metric)
        case _:
            raise ValueError(f"Unsupported web metric: {parsed_metric}")


def _json_leaderboard_response(
    parsed_metric: Metric,
    *,
    recalculate: bool,
):
    match parsed_metric:
        case Metric.RAWR:
            return json_rawr_leaderboard_response(recalculate=recalculate)
        case Metric.WOWY | Metric.WOWY_SHRUNK:
            return json_wowy_leaderboard_response(parsed_metric, recalculate=recalculate)
        case _:
            raise ValueError(f"Unsupported web metric: {parsed_metric}")


def _csv_leaderboard_response(
    parsed_metric: Metric,
    *,
    recalculate: bool,
):
    match parsed_metric:
        case Metric.RAWR:
            return csv_rawr_leaderboard_response(recalculate=recalculate)
        case Metric.WOWY | Metric.WOWY_SHRUNK:
            return csv_wowy_leaderboard_response(parsed_metric, recalculate=recalculate)
        case _:
            raise ValueError(f"Unsupported web metric: {parsed_metric}")


def register_metric_routes(app: Flask) -> None:
    @app.get("/api/metrics/<metric>/options")
    @web_route
    def get_metric_options(metric: str):
        return _json_options_response(Metric.parse(metric))

    @app.get("/api/metrics/<metric>/player-seasons")
    @web_route
    def get_metric_player_seasons(metric: str):
        return _json_player_seasons_response(Metric.parse(metric))

    @app.get("/api/metrics/<metric>/span-chart")
    @web_route
    def get_metric_span_chart(metric: str):
        return _json_span_chart_response(Metric.parse(metric))

    @app.get("/api/metrics/<metric>/cached-leaderboard")
    @web_route
    def get_metric_cached_leaderboard(metric: str):
        return _json_leaderboard_response(Metric.parse(metric), recalculate=False)

    @app.get("/api/metrics/<metric>/cached-leaderboard.csv")
    @web_route
    def export_metric_cached_leaderboard(metric: str):
        return _csv_leaderboard_response(Metric.parse(metric), recalculate=False)

    @app.get("/api/metrics/<metric>/custom-query")
    @web_route
    def get_metric_custom_query(metric: str):
        return _json_leaderboard_response(Metric.parse(metric), recalculate=True)

    @app.get("/api/metrics/<metric>/custom-query.csv")
    @web_route
    def export_metric_custom_query(metric: str):
        return _csv_leaderboard_response(Metric.parse(metric), recalculate=True)

    @app.get("/api/metrics/<metric>/leaderboard")
    @web_route
    def get_metric_leaderboard(metric: str):
        return _json_leaderboard_response(Metric.parse(metric), recalculate=False)

    @app.get("/api/metrics/<metric>/leaderboard.csv")
    @web_route
    def export_metric_leaderboard(metric: str):
        return _csv_leaderboard_response(Metric.parse(metric), recalculate=False)
