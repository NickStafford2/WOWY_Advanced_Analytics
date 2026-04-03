from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from rawr_analytics.cli._ingest_terminal import (
    render_failure_summary,
    render_progress_line,
    render_team_complete_line,
)
from rawr_analytics.cli._rebuild_terminal import (
    render_metric_progress as _render_metric_progress_bar,
    render_rebuild_team_failure,
    render_validation_progress as _render_validation_progress_bar,
)
from rawr_analytics.services import (
    IngestResult,
    RebuildTeamFailureEvent,
    SeasonRangeFailure,
    format_rebuild_validation_summary,
    parse_rebuild_request,
    rebuild_player_metrics_db,
)

_DEFAULT_START_YEAR = 2025
_DEFAULT_END_YEAR = 1998


def build_parser() -> argparse.ArgumentParser:
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
    parser = build_parser()
    args = parser.parse_args(argv)

    result = rebuild_player_metrics_db(
        parse_rebuild_request(
            start_year=args.start_year,
            end_year=args.end_year,
            season_type=args.season_type,
            teams=args.teams,
            metrics=args.metric,
            keep_existing_db=args.keep_existing_db,
        ),
        ingest_progress_fn=render_progress_line,
        season_started_fn=_render_season_started,
        team_completed_fn=_render_team_completed,
        team_failed_fn=_render_team_failed,
        metric_progress_fn=_render_metric_progress,
        validation_progress_fn=_render_validation_progress,
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


def _render_season_started(season_index: int, season_count: int, season: str) -> None:
    if season_count > 1:
        print(f"[{season_index}/{season_count}] caching {season}")


def _render_team_completed(team_index: int, team_total: int, result: IngestResult) -> None:
    render_team_complete_line(team_index, team_total, result)
    sys.stdout.write("\n")


def _render_team_failed(event: RebuildTeamFailureEvent) -> None:
    render_rebuild_team_failure(event)


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


def _render_metric_progress(metric: str, current: int, total: int, detail: str) -> None:
    _render_metric_progress_bar(metric, current, total, detail)


def _render_validation_progress(current: int, total: int, label: str) -> None:
    _render_validation_progress_bar(current, total, label)


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
