from __future__ import annotations

from collections.abc import Callable
from functools import wraps

from flask import Flask, Response, jsonify

from rawr_analytics.metrics.constants import Metric


class _WebBadRequestError(ValueError):
    pass


def register_error_handlers(app: Flask) -> None:
    @app.errorhandler(_WebBadRequestError)
    def _handle_bad_request(exc: _WebBadRequestError) -> tuple[Response, int]:
        return jsonify({"error": str(exc)}), 400


def _require_metric[T](metric: T) -> T:
    if not isinstance(metric, Metric):
        raise _WebBadRequestError("invalid metric")
    return metric


def web_route[**P, R: Response | tuple[Response, int]](
    route_fn: Callable[P, R],
) -> Callable[P, R]:
    @wraps(route_fn)
    def _wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        return route_fn(*args, **kwargs)

    return _wrapped
