from __future__ import annotations

from flask import Flask

from rawr_analytics.web._errors import register_error_handlers
from rawr_analytics.web.routes.rawr_routes import register_rawr_routes
from rawr_analytics.web.routes.wowy_routes import register_wowy_routes
from rawr_analytics.web.routes.wowy_shrunk_routes import register_wowy_shrunk_routes


def create_app() -> Flask:
    app = Flask(__name__)
    register_error_handlers(app)

    register_rawr_routes(app)
    register_wowy_routes(app)
    register_wowy_shrunk_routes(app)

    return app
