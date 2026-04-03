from __future__ import annotations

import argparse
import sys

from rawr_analytics.cli import (
    render_failure_summary,
    render_partial_failure_details,
    render_progress_line,
    render_team_complete_line,
    render_team_fetch_failed_line,
    render_team_partial_failed_line,
    render_team_validation_failed_line,
)
from rawr_analytics.metrics.constants import Metric
from rawr_analytics.nba import FetchError, PartialTeamSeasonError, append_ingest_failure_log
from rawr_analytics.progress import TerminalProgressBar
from rawr_analytics.services import (
    IngestResult,
    RebuildRequest,
    SeasonRangeFailure,
    format_rebuild_validation_summary,
    rebuild_player_metrics_db,
)
from rawr_analytics.shared.season import SeasonType

_DEFAULT_START_YEAR = 2025
_DEFAULT_END_YEAR = 1998
_METRIC_PROGRESS_BARS: dict[Metric, TerminalProgressBar] = {}
_VALIDATION_PROGRESS_BAR: TerminalProgressBar | None = None


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

    if args.start_year < args.end_year:
        raise ValueError("Start year must be greater than or equal to end year")

    result = rebuild_player_metrics_db(
        RebuildRequest(
            start_year=args.start_year,
            end_year=args.end_year,
            season_type=SeasonType.parse(args.season_type),
            teams=args.teams,
            metrics=[Metric.parse(metric) for metric in args.metric] if args.metric else None,
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


def _render_season_started(season_index: int, season_count: int, season: object) -> None:
    if season_count > 1:
        print(f"[{season_index}/{season_count}] caching {season}")


def _render_team_completed(team_index: int, team_total: int, result: IngestResult) -> None:
    render_team_complete_line(team_index, team_total, result)
    sys.stdout.write("\n")


def _render_team_failed(team_index: int, team_total: int, failure: SeasonRangeFailure) -> None:
    request = failure.request
    team = request.team
    season = request.season
    error = failure.error

    append_ingest_failure_log(
        team=team,
        season=season,
        failure_kind=failure.failure_kind,
        error=error,
    )

    if failure.failure_kind == "fetch_error":
        assert isinstance(error, FetchError)
        render_team_fetch_failed_line(
            team_index=team_index,
            team_total=team_total,
            team=team,
            season=season,
            error_type=error.last_error_type,
        )
        sys.stdout.write("\n")
        sys.stderr.write(f"Fetch failed for {request.label}: {error}\n")
        sys.stderr.flush()
        return

    if failure.failure_kind == "partial_scope_error":
        assert isinstance(error, PartialTeamSeasonError)
        render_team_partial_failed_line(
            team_index=team_index,
            team_total=team_total,
            team=team,
            season=season,
            failed_games=error.failed_games,
            total_games=error.total_games,
        )
        sys.stdout.write("\n")
        sys.stderr.write(
            f"Incomplete cache for {request.label}: "
            f"{error.failed_games}/{error.total_games} games failed normalization\n"
        )
        sys.stderr.write(f"{render_partial_failure_details(error)}\n")
        sys.stderr.flush()
        return

    reason = str(error)
    render_team_validation_failed_line(
        team_index=team_index,
        team_total=team_total,
        team=team,
        season=season,
        reason=reason,
    )
    sys.stdout.write("\n")
    sys.stderr.write(f"Validation failed for {request.label}: {reason}\n")
    sys.stderr.flush()


def _render_failure_summary(failures: list[SeasonRangeFailure]) -> None:
    if not failures:
        return
    counts: dict[str, int] = {}
    for failure in failures:
        counts[failure.failure_kind] = counts.get(failure.failure_kind, 0) + 1
    render_failure_summary(
        failure_counts=counts,
        failed_scopes=[failure.scope for failure in failures],
    )


def _render_metric_progress(metric: Metric, current: int, total: int, detail: str) -> None:
    progress_bar = _METRIC_PROGRESS_BARS.get(metric)
    if progress_bar is None:
        progress_bar = TerminalProgressBar(f"Refresh {metric.value}", total=max(total, 1))
        _METRIC_PROGRESS_BARS[metric] = progress_bar
    progress_bar.total = max(total, 1)
    progress_bar.update(current, detail=detail)
    if current >= total:
        progress_bar.finish(detail="done")
        del _METRIC_PROGRESS_BARS[metric]


def _render_validation_progress(current: int, total: int, label: str) -> None:
    global _VALIDATION_PROGRESS_BAR
    if _VALIDATION_PROGRESS_BAR is None:
        _VALIDATION_PROGRESS_BAR = TerminalProgressBar(
            "Validate rebuilt database",
            total=max(total, 1),
        )
    progress_bar = _VALIDATION_PROGRESS_BAR
    progress_bar.total = max(total, 1)
    progress_bar.update(current, detail=label)
    if current >= total:
        progress_bar.finish(detail="done")
        _VALIDATION_PROGRESS_BAR = None


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
