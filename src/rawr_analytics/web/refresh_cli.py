from __future__ import annotations

import argparse

from rawr_analytics.progress import TerminalProgressBar, print_status_box
from rawr_analytics.web.metric_store import (
    DEFAULT_RAWR_RIDGE_ALPHA,
    RAWR_METRIC,
    WOWY_METRIC,
    WOWY_SHRUNK_METRIC,
    refresh_metric_store,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the SQLite metric store used by the web app."
    )
    parser.add_argument(
        "--metric",
        action="append",
        choices=[WOWY_METRIC, WOWY_SHRUNK_METRIC, RAWR_METRIC],
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
    parser = build_parser()
    args = parser.parse_args(argv)
    metrics = args.metric or [WOWY_METRIC, WOWY_SHRUNK_METRIC, RAWR_METRIC]
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
            metric,
            season_type=args.season_type,
            db_path=args.player_metrics_db_path,
            rawr_ridge_alpha=args.rawr_ridge_alpha,
            include_team_scopes=False,
            progress=lambda current, total, detail, progress_bar=progress_bar: _update_progress(
                progress_bar,
                current=current,
                total=total,
                detail=detail,
            ),
        )
        progress_bar.finish(detail="done")
        if not result.ok:
            print(f"failed to refresh {metric} store at {args.player_metrics_db_path}")
            print(result.failure_message)
            return 1
        print(
            f"refreshed {metric} store at {args.player_metrics_db_path} ({result.total_rows} rows)"
        )
    return 0


def _update_progress(
    progress_bar: TerminalProgressBar,
    *,
    current: int,
    total: int,
    detail: str,
) -> None:
    progress_bar.total = max(total, 1)
    progress_bar.update(current, detail=detail)


if __name__ == "__main__":
    raise SystemExit(main())
