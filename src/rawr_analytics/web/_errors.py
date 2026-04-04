from __future__ import annotations

from collections.abc import Callable
from functools import wraps

from flask import Flask, Response, jsonify


class WebBadRequestError(ValueError):
    pass


def register_error_handlers(app: Flask) -> None:
    @app.errorhandler(WebBadRequestError)
    def _handle_bad_request(exc: WebBadRequestError) -> tuple[Response, int]:
        return jsonify({"error": str(exc)}), 400


def web_route[**P, R: Response | tuple[Response, int]](
    route_fn: Callable[P, R],
) -> Callable[P, R]:
    @wraps(route_fn)
    def _wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return route_fn(*args, **kwargs)
        except ValueError as exc:
            raise WebBadRequestError(str(exc)) from exc

    return _wrapped
