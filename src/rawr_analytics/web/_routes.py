from __future__ import annotations

from collections.abc import Callable

from flask import Flask, Response

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.web._errors import web_route
from rawr_analytics.web._rawr_handlers import (
    csv_leaderboard_response as rawr_csv_leaderboard_response,
)
from rawr_analytics.web._rawr_handlers import (
    json_leaderboard_response as rawr_json_leaderboard_response,
)
from rawr_analytics.web._rawr_handlers import (
    json_options_response as rawr_json_options_response,
)
from rawr_analytics.web._rawr_handlers import (
    json_player_seasons_response as rawr_json_player_seasons_response,
)
from rawr_analytics.web._rawr_handlers import (
    json_span_chart_response as rawr_json_span_chart_response,
)
from rawr_analytics.web._wowy_handlers import (
    csv_leaderboard_response as wowy_csv_leaderboard_response,
)
from rawr_analytics.web._wowy_handlers import (
    json_leaderboard_response as wowy_json_leaderboard_response,
)
from rawr_analytics.web._wowy_handlers import (
    json_options_response as wowy_json_options_response,
)
from rawr_analytics.web._wowy_handlers import (
    json_player_seasons_response as wowy_json_player_seasons_response,
)
from rawr_analytics.web._wowy_handlers import (
    json_span_chart_response as wowy_json_span_chart_response,
)


def _dispatch_metric(
    metric_name: str,
    *,
    rawr_response: Callable[[], Response],
    wowy_response: Callable[[Metric], Response],
) -> Response:
    metric = Metric.parse(metric_name)
    match metric:
        case Metric.RAWR:
            return rawr_response()
        case Metric.WOWY | Metric.WOWY_SHRUNK:
            return wowy_response(metric)
        case _:
            raise ValueError(f"Unsupported web metric: {metric}")


def _json_leaderboard_response(metric_name: str, *, recalculate: bool) -> Response:
    metric = Metric.parse(metric_name)
    match metric:
        case Metric.RAWR:
            return rawr_json_leaderboard_response(recalculate)
        case Metric.WOWY | Metric.WOWY_SHRUNK:
            return wowy_json_leaderboard_response(metric, recalculate)
        case _:
            raise ValueError(f"Unsupported web metric: {metric}")


def _csv_leaderboard_response(metric_name: str, *, recalculate: bool) -> Response:
    metric = Metric.parse(metric_name)
    match metric:
        case Metric.RAWR:
            return rawr_csv_leaderboard_response(recalculate)
        case Metric.WOWY | Metric.WOWY_SHRUNK:
            return wowy_csv_leaderboard_response(metric, recalculate)
        case _:
            raise ValueError(f"Unsupported web metric: {metric}")


def register_metric_routes(app: Flask) -> None:
    @app.get("/api/metrics/<metric>/options")
    @web_route
    def get_metric_options(metric: str):
        return _dispatch_metric(
            metric,
            rawr_response=rawr_json_options_response,
            wowy_response=wowy_json_options_response,
        )

    @app.get("/api/metrics/<metric>/player-seasons")
    @web_route
    def get_metric_player_seasons(metric: str):
        return _dispatch_metric(
            metric,
            rawr_response=rawr_json_player_seasons_response,
            wowy_response=wowy_json_player_seasons_response,
        )

    @app.get("/api/metrics/<metric>/span-chart")
    @web_route
    def get_metric_span_chart(metric: str):
        return _dispatch_metric(
            metric,
            rawr_response=rawr_json_span_chart_response,
            wowy_response=wowy_json_span_chart_response,
        )

    @app.get("/api/metrics/<metric>/leaderboard")
    @app.get("/api/metrics/<metric>/cached-leaderboard")
    @web_route
    def get_metric_cached_leaderboard(metric: str):
        return _json_leaderboard_response(metric, recalculate=False)

    @app.get("/api/metrics/<metric>/leaderboard.csv")
    @app.get("/api/metrics/<metric>/cached-leaderboard.csv")
    @web_route
    def export_metric_cached_leaderboard(metric: str):
        return _csv_leaderboard_response(metric, recalculate=False)

    @app.get("/api/metrics/<metric>/custom-query")
    @web_route
    def get_metric_custom_query(metric: str):
        return _json_leaderboard_response(metric, recalculate=True)

    @app.get("/api/metrics/<metric>/custom-query.csv")
    @web_route
    def export_metric_custom_query(metric: str):
        return _csv_leaderboard_response(metric, recalculate=True)
