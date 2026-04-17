from __future__ import annotations

import queue
import threading
import uuid
from dataclasses import asdict
from typing import Any

from flask import Flask, Response, jsonify, request

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.wowy.progress import WowyProgressUpdate
from rawr_analytics.metrics.wowy.query.presenters import build_wowy_export_rows
from rawr_analytics.metrics.wowy.query.service import (
    build_wowy_leaderboard_payload,
    build_wowy_options_payload,
    build_wowy_player_seasons_payload,
    build_wowy_span_chart_payload,
    resolve_wowy_result,
)
from rawr_analytics.web._csv import render_leaderboard_csv
from rawr_analytics.web._errors import web_route
from rawr_analytics.web._parse import (
    build_wowy_options_query_from_request,
    build_wowy_query_from_request,
)
from rawr_analytics.web.sse import (
    format_sse_comment,
    format_sse_event,
    sse_headers,
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

    @app.get("/api/metrics/wowy/leaderboard/stream")
    @app.get("/api/metrics/wowy/cached-leaderboard/stream")
    def get_wowy_cached_leaderboard_stream():
        return sse_leaderboard_response(Metric.WOWY, recalculate=False)

    @app.get("/api/metrics/wowy/leaderboard.csv")
    @app.get("/api/metrics/wowy/cached-leaderboard.csv")
    @web_route
    def export_wowy_cached_leaderboard():
        return csv_leaderboard_response(Metric.WOWY, recalculate=False)

    @app.get("/api/metrics/wowy/custom-query")
    @web_route
    def get_wowy_custom_query():
        return json_leaderboard_response(Metric.WOWY, recalculate=True)

    @app.get("/api/metrics/wowy/custom-query/stream")
    def get_wowy_custom_query_stream():
        return sse_leaderboard_response(Metric.WOWY, recalculate=True)

    @app.get("/api/metrics/wowy/custom-query.csv")
    @web_route
    def export_wowy_custom_query():
        return csv_leaderboard_response(Metric.WOWY, recalculate=True)


def json_options_response(metric: Metric) -> Response:
    query = build_wowy_options_query_from_request(request)
    payload = build_wowy_options_payload(query=query, metric=metric)
    return jsonify(payload)


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


def sse_leaderboard_response(
    metric: Metric,
    *,
    recalculate: bool = False,
) -> Response:
    query = build_wowy_query_from_request(request)
    stream_id = str(uuid.uuid4())
    event_queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()

    def publish_progress(update: WowyProgressUpdate) -> None:
        event_queue.put(
            (
                "progress",
                {
                    "stream_id": stream_id,
                    **asdict(update),
                },
            )
        )

    def run_query() -> None:
        succeeded = False
        try:
            event_queue.put(
                (
                    "started",
                    {
                        "stream_id": stream_id,
                        "metric": metric.value,
                        "recalculate": recalculate,
                    },
                )
            )
            result = resolve_wowy_result(
                query,
                metric=metric,
                recalculate=recalculate,
                progress_sink=publish_progress,
            )
            payload = build_wowy_leaderboard_payload(query, result)
            event_queue.put(
                (
                    "result",
                    {
                        "stream_id": stream_id,
                        "payload": payload,
                    },
                )
            )
            succeeded = True
        except Exception as exc:
            event_queue.put(
                (
                    "error",
                    {
                        "stream_id": stream_id,
                        "message": str(exc),
                    },
                )
            )
        finally:
            if succeeded:
                event_queue.put(("done", {"stream_id": stream_id}))

    def generate():
        worker = threading.Thread(target=run_query, daemon=True)
        worker.start()

        yield format_sse_comment(f"{metric.value} stream {stream_id} opened")

        while True:
            event_name, payload = event_queue.get()
            if event_name == "done":
                break
            yield format_sse_event(event=event_name, data=payload)
            if event_name in {"result", "error"}:
                break

    return Response(generate(), headers=sse_headers())


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
            table_rows=build_wowy_export_rows(
                rows=result.player_season_value, seasons=result.seasons
            ),
        ),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
