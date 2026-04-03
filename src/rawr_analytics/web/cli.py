from __future__ import annotations

import argparse

from rawr_analytics.services import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    DEFAULT_WEB_METRIC_IDS,
    MetricStoreRefreshProgressEvent,
    build_metric_store_refresh_request,
    refresh_metric_store,
)
from rawr_analytics.web.app import create_app


def _build_parser() -> argparse.ArgumentParser:
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
        choices=list(DEFAULT_WEB_METRIC_IDS),
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
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.refresh_store:
        refresh_metrics = args.refresh_metric or list(DEFAULT_WEB_METRIC_IDS)
        for metric in refresh_metrics:
            print(f"refreshing {metric} web store")
            result = refresh_metric_store(
                build_metric_store_refresh_request(
                    metric=metric,
                    season_type=args.season_type,
                    rawr_ridge_alpha=args.rawr_ridge_alpha,
                    include_team_scopes=False,
                ),
                event_fn=lambda event, metric=metric: _print_refresh_progress(metric, event),
            )
            if not result.ok:
                print(result.failure_message)
                return 1
    app = create_app()
    app.run(host=args.host, port=args.port, debug=args.debug)
    return 0


def _print_refresh_progress(metric: str, event: MetricStoreRefreshProgressEvent) -> None:
    if event.total <= 0:
        return
    print(f"[{metric}] {event.current}/{event.total} {event.detail}")
