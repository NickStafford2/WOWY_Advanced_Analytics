from __future__ import annotations

from flask import Flask

from rawr_analytics.web._errors import register_error_handlers
from rawr_analytics.web._routes import register_metric_routes


def create_app() -> Flask:
    app = Flask(__name__)
    register_error_handlers(app)
    register_metric_routes(app)
    return app
