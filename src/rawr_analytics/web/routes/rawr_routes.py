from __future__ import annotations

import queue
import threading
import uuid
from dataclasses import asdict
from typing import Any

from flask import Flask, Response, jsonify, request

from rawr_analytics.metrics.constants import Metric
from rawr_analytics.metrics.rawr.progress import RawrProgressUpdate
from rawr_analytics.metrics.rawr.query.presenters import build_rawr_export_rows
from rawr_analytics.metrics.rawr.query.service import (
    build_rawr_leaderboard_payload,
    build_rawr_options_payload,
    build_rawr_player_seasons_payload,
    build_rawr_span_chart_payload,
    resolve_rawr_result,
)
from rawr_analytics.web._csv import render_leaderboard_csv
from rawr_analytics.web._errors import web_route
from rawr_analytics.web._parse import (
    build_rawr_options_query_from_request,
    build_rawr_query_from_request,
)
from rawr_analytics.web.sse import (
    format_sse_comment,
    format_sse_event,
    sse_headers,
)


def register_rawr_routes(app: Flask) -> None:
    @app.get("/api/metrics/rawr/options")
    @web_route
    def get_rawr_options():
        query = build_rawr_options_query_from_request(request)
        payload = build_rawr_options_payload(query)
        return jsonify(payload)

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

    @app.get("/api/metrics/rawr/leaderboard/stream")
    @app.get("/api/metrics/rawr/cached-leaderboard/stream")
    def get_rawr_cached_leaderboard_stream():
        return _rawr_sse_leaderboard_response(recalculate=False)

    @app.get("/api/metrics/rawr/leaderboard.csv")
    @app.get("/api/metrics/rawr/cached-leaderboard.csv")
    @web_route
    def export_rawr_cached_leaderboard():
        return _rawr_csv_leaderboard_response(recalculate=False)

    @app.get("/api/metrics/rawr/custom-query")
    @web_route
    def get_rawr_custom_query():
        return _rawr_json_leaderboard_response(recalculate=True)

    @app.get("/api/metrics/rawr/custom-query/stream")
    def get_rawr_custom_query_stream():
        return _rawr_sse_leaderboard_response(recalculate=True)

    @app.get("/api/metrics/rawr/custom-query.csv")
    @web_route
    def export_rawr_custom_query():
        return _rawr_csv_leaderboard_response(recalculate=True)


def _rawr_json_leaderboard_response(*, recalculate: bool = False) -> Response:
    query = build_rawr_query_from_request(request)
    result = resolve_rawr_result(query, recalculate=recalculate)
    return jsonify(build_rawr_leaderboard_payload(query, result, recalculate=recalculate))


def _rawr_sse_leaderboard_response(*, recalculate: bool = False) -> Response:
    query = build_rawr_query_from_request(request)
    stream_id = str(uuid.uuid4())
    event_queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()

    def publish_progress(update: RawrProgressUpdate) -> None:
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
                        "metric": Metric.RAWR.value,
                        "recalculate": recalculate,
                    },
                )
            )
            result = resolve_rawr_result(
                query,
                progress_sink=publish_progress,
                recalculate=recalculate,
            )
            payload = build_rawr_leaderboard_payload(
                query,
                result,
                recalculate=recalculate,
            )
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

        yield format_sse_comment(f"rawr stream {stream_id} opened")

        while True:
            event_name, payload = event_queue.get()
            if event_name == "done":
                break
            yield format_sse_event(event_name, payload)
            if event_name in {"result", "error"}:
                break

    return Response(generate(), headers=sse_headers())


def _rawr_csv_leaderboard_response(*, recalculate: bool = False) -> Response:
    query = build_rawr_query_from_request(request)
    result = resolve_rawr_result(query, recalculate=recalculate)
    filename = f"{Metric.RAWR.value}-all-players.csv"
    return Response(
        render_leaderboard_csv(
            metric=Metric.RAWR,
            table_rows=build_rawr_export_rows(rows=result.rows, seasons=result.seasons),
        ),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
