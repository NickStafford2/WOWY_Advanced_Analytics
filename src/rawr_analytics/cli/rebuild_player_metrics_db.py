from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from rawr_analytics.app.rebuild import (
    format_rebuild_validation_summary,
    rebuild_player_metrics_db,
)
from rawr_analytics.cli._failure_logging import append_failure_log_entry
from rawr_analytics.cli._ingest_terminal import (
    render_failure_summary,
)
from rawr_analytics.cli._rebuild_terminal import (
    render_rebuild_event,
)
from rawr_analytics.sources.nba_api.ingest._models import SeasonRangeFailure

_DEFAULT_START_YEAR = 2025
_DEFAULT_END_YEAR = 1998


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild the full player metrics SQLite database from scratch: "
            "normalized NBA cache, web metric store, and validation."
        )
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=_DEFAULT_START_YEAR,
        help=f"Latest season start year to rebuild (default: {_DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=_DEFAULT_END_YEAR,
        help=f"Earliest season start year to rebuild (default: {_DEFAULT_END_YEAR})",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type to rebuild.",
    )
    parser.add_argument(
        "--teams",
        nargs="*",
        default=None,
        help="Optional team abbreviations to restrict the rebuild scope.",
    )
    parser.add_argument(
        "--metric",
        action="append",
        choices=["wowy", "wowy_shrunk", "rawr"],
        help="Optional web metric to refresh. Repeat to select multiple. Defaults to all.",
    )
    parser.add_argument(
        "--keep-existing-db",
        action="store_true",
        help="Do not delete the existing database before rebuilding.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    result = rebuild_player_metrics_db(
        start_year=args.start_year,
        end_year=args.end_year,
        season_type=args.season_type,
        teams=args.teams,
        metrics=args.metric,
        keep_existing_db=args.keep_existing_db,
        event_fn=render_rebuild_event,
        failure_log_fn=append_failure_log_entry,
    )
    if result.deleted_existing_db:
        print("Deleted existing database before rebuild.")

    print("\n== Ingest refresh ==")
    print(
        "completed "
        f"{result.ingest_result.completed_team_seasons}/"
        f"{result.ingest_result.attempted_team_seasons} team-seasons"
    )
    _render_failure_summary(result.ingest_result.failures)

    print("\n== Metric-store refresh ==")
    for metric_result in result.metric_results:
        status = "ok" if metric_result.ok else "failed"
        print(f"{metric_result.metric.value}: {status} ({metric_result.total_rows} rows)")
        for warning in metric_result.warnings:
            print(f"warning: {warning}")

    if result.validation_summary is not None:
        print("\n== Validation ==")
        print(format_rebuild_validation_summary(result.validation_summary, top_n=10))

    if result.failure_message is not None:
        print(f"\nRebuild failed: {result.failure_message}")
        return 1

    print("\nRebuild complete.")
    return 0


def _render_failure_summary(failures: Sequence[SeasonRangeFailure]) -> None:
    if not failures:
        return
    counts: dict[str, int] = {}
    for failure in failures:
        counts[failure.failure_kind] = counts.get(failure.failure_kind, 0) + 1
    render_failure_summary(
        failure_counts=counts,
        failed_scopes=[failure.scope for failure in failures],
    )


def _run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(_run())
