from __future__ import annotations

from flask import Response, jsonify, request

from rawr_analytics.app.rawr.service import (
    build_rawr_leaderboard_export,
    build_rawr_leaderboard_payload,
    build_rawr_options_payload,
    build_rawr_player_seasons_payload,
    build_rawr_span_chart_payload,
    resolve_rawr_result,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.web._parse import (
    build_rawr_options_query_from_request,
    resolve_rawr_query_from_request,
)
from rawr_analytics.web.csv import render_leaderboard_csv


def json_options_response() -> Response:
    return jsonify(build_rawr_options_payload(build_rawr_options_query_from_request(request)))


def json_player_seasons_response() -> Response:
    query = resolve_rawr_query_from_request(request)
    result = resolve_rawr_result(query)
    return jsonify(build_rawr_player_seasons_payload(query, result))


def json_span_chart_response() -> Response:
    query = resolve_rawr_query_from_request(request)
    result = resolve_rawr_result(query)
    return jsonify(build_rawr_span_chart_payload(query, result))


def json_leaderboard_response(
    *,
    recalculate: bool = False,
) -> Response:
    query = resolve_rawr_query_from_request(request, recalculate=recalculate)
    result = resolve_rawr_result(query)
    return jsonify(build_rawr_leaderboard_payload(query, result))


def csv_leaderboard_response(
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
