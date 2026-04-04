from __future__ import annotations

from functools import partial

from flask import Response, jsonify, request

from rawr_analytics.app.wowy.service import (
    build_wowy_leaderboard_export,
    build_wowy_leaderboard_payload,
    build_wowy_options_payload,
    build_wowy_player_seasons_payload,
    build_wowy_span_chart_payload,
    resolve_wowy_result,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.web._parse import (
    build_wowy_options_query_from_request,
    build_wowy_query_from_request,
)
from rawr_analytics.web._metric_handlers import MetricWebHandlers
from rawr_analytics.web.csv import render_leaderboard_csv


def build_metric_handlers(metric: Metric) -> MetricWebHandlers:
    assert metric in {Metric.WOWY, Metric.WOWY_SHRUNK}
    return MetricWebHandlers(
        json_options_response=partial(json_options_response, metric),
        json_player_seasons_response=partial(json_player_seasons_response, metric),
        json_span_chart_response=partial(json_span_chart_response, metric),
        json_leaderboard_response=partial(json_leaderboard_response, metric),
        csv_leaderboard_response=partial(csv_leaderboard_response, metric),
    )


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
    recalculate: bool = False,
) -> Response:
    query = build_wowy_query_from_request(request)
    result = resolve_wowy_result(query, metric=metric, recalculate=recalculate)
    return jsonify(build_wowy_leaderboard_payload(query, result))


def csv_leaderboard_response(
    metric: Metric,
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
