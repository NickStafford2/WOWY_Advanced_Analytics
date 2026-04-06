from __future__ import annotations

import argparse

from rawr_analytics.app.metric_store import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    MetricStoreRefreshProgressEvent,
    refresh_metric_store,
)
from rawr_analytics.cli._progress_bar import TerminalProgressBar, print_status_box
from rawr_analytics.metrics.constants import Metric

_choices = [m.value for m in Metric]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the SQLite metric store used by the web app."
    )
    parser.add_argument(
        "--metric",
        action="append",
        choices=_choices,
        help=(
            "Metric to refresh into the SQLite store. "
            "Repeat to refresh multiple metrics. Defaults to all metrics."
        ),
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to build.",
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
    metrics = args.metric or _choices
    print_status_box(
        "Web Store Refresh",
        [
            f"Metrics: {', '.join(metrics)}",
            "Refreshing cached player-season rows and full-span leaderboard"
            " slices used by the Flask and React web app.",
            "The progress bar below tracks each built team scope in the SQLite metric store.",
        ],
    )
    for metric in metrics:
        progress_bar = TerminalProgressBar(f"Refresh {metric}", total=1)
        result = refresh_metric_store(
            metric=metric,
            season_type=args.season_type,
            rawr_ridge_alpha=args.rawr_ridge_alpha,
            include_team_scopes=False,
            event_fn=lambda event, progress_bar=progress_bar: _update_progress(
                progress_bar,
                event,
            ),
        )
        progress_bar.finish(detail="done")
        if not result.ok:
            print(f"failed to refresh {metric} store")
            print(result.failure_message)
            return 1
        print(f"refreshed {metric} store ({result.total_rows} rows)")
    return 0


def _update_progress(
    progress_bar: TerminalProgressBar,
    event: MetricStoreRefreshProgressEvent,
) -> None:
    progress_bar.total = max(event.total, 1)
    progress_bar.update(event.current, detail=event.detail)


if __name__ == "__main__":
    raise SystemExit(main())
