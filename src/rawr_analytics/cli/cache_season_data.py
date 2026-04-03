from __future__ import annotations

import argparse
import sys

from rawr_analytics.cli._ingest_terminal import (
    render_failure_summary,
    render_ingest_failure,
    render_progress_line,
    render_team_complete_line,
)
from rawr_analytics.services.ingest import (
    IngestRefreshRequest,
    IngestResult,
    SeasonRangeFailure,
    SeasonRangeResult,
    refresh_season_range,
)
from rawr_analytics.shared import Season

_DEFAULT_START_YEAR = 2000
_DEFAULT_END_YEAR = 1946
_DEFAULT_SEASON_TYPE = "Regular Season"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch, normalize, and cache all NBA seasons by default, or one season."
    )
    parser.add_argument(
        "season",
        nargs="?",
        help="Optional NBA season string, for example 2023-24. If omitted, refreshes all seasons.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=_DEFAULT_START_YEAR,
        help=f"First season start year to cache (default: {_DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=_DEFAULT_END_YEAR,
        help=(f"Earliest season end year to cache, inclusive (default: {_DEFAULT_END_YEAR})"),
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        help="NBA season type, for example 'Regular Season' or 'Playoffs'",
    )
    parser.add_argument(
        "--teams",
        nargs="*",
        default=None,
        help="Optional team abbreviations. If omitted, fetches all NBA teams.",
    )
    parser.add_argument(
        "--skip-combine",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    return parser


def _render_season_started(season_index: int, season_count: int, season: Season) -> None:
    if season_count > 1:
        print(f"[{season_index}/{season_count}] caching {season}")


def _render_team_completed(team_index: int, team_total: int, result: IngestResult) -> None:
    render_team_complete_line(team_index, team_total, result)
    sys.stdout.write("\n")


def _render_team_failed(team_index: int, team_total: int, failure: SeasonRangeFailure) -> None:
    render_ingest_failure(team_index, team_total, failure)


def _render_failure_summary_for_result(result: SeasonRangeResult) -> None:
    if not result.failures:
        return
    render_failure_summary(
        failure_counts=result.failure_counts,
        failed_scopes=result.failed_scopes,
    )


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    result = refresh_season_range(
        IngestRefreshRequest(
            season_str=args.season,
            start_year=args.start_year or _DEFAULT_START_YEAR,
            end_year=args.end_year or _DEFAULT_END_YEAR,
            season_type=args.season_type or _DEFAULT_SEASON_TYPE,
            team_abbreviations=args.teams,
        ),
        progress_fn=render_progress_line,
        season_started_fn=_render_season_started,
        team_completed_fn=_render_team_completed,
        team_failed_fn=_render_team_failed,
    )
    _render_failure_summary_for_result(result)
    return result.exit_status


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
