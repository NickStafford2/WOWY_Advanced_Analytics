from __future__ import annotations

import argparse

from rawr_analytics.metrics.store import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    RAWR_METRIC,
    WOWY_METRIC,
    WOWY_SHRUNK_METRIC,
    refresh_metric_store,
)
from rawr_analytics.nba.season_types import canonicalize_season_type
from rawr_analytics.web.app import create_app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the WOWY Flask backend for web development.")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host interface to bind the Flask development server.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port to bind the Flask development server.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable Flask debug mode.",
    )
    parser.add_argument(
        "--refresh-store",
        action="store_true",
        help="Refresh cached web metric stores before starting the server.",
    )
    parser.add_argument(
        "--refresh-metric",
        action="append",
        choices=[WOWY_METRIC, WOWY_SHRUNK_METRIC, RAWR_METRIC],
        help=(
            "Metric to refresh when used with --refresh-store. "
            "Repeat to refresh multiple metrics. Defaults to refreshing both."
        ),
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to build for the cached leaderboard store.",
    )
    parser.add_argument(
        "--rawr-ridge-alpha",
        type=float,
        default=DEFAULT_RAWR_RIDGE_ALPHA,
        help="Ridge alpha used when building cached RAWR web rows.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    season_type = canonicalize_season_type(args.season_type)
    if args.refresh_store:
        refresh_metrics = args.refresh_metric or [
            WOWY_METRIC,
            WOWY_SHRUNK_METRIC,
            RAWR_METRIC,
        ]
        for metric in refresh_metrics:
            print(f"refreshing {metric} web store")
            result = refresh_metric_store(
                metric,
                season_type=season_type,
                rawr_ridge_alpha=args.rawr_ridge_alpha,
                include_team_scopes=False,
            )
            if not result.ok:
                print(result.failure_message)
                return 1
    app = create_app()
    app.run(host=args.host, port=args.port, debug=args.debug)
    return 0
