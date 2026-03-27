from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rawr_analytics.nba.errors import (
    FetchError,
    PartialTeamSeasonError,
)
from rawr_analytics.nba.ingest_logging import (
    DEFAULT_INGEST_FAILURE_LOG_PATH,
    append_ingest_failure_log,
)
from rawr_analytics.shared.season import Season, build_season_list
from rawr_analytics.shared.team import Team
from rawr_analytics.workflows.nba_ingest import IngestRequest, refresh
from scripts._render_cli import (
    filtered_log,
    record_failure,
    render_failure_summary,
    render_partial_failure_details,
    render_progress_line,
    render_team_complete_line,
    render_team_fetch_failed_line,
    render_team_partial_failed_line,
    render_team_validation_failed_line,
)

_DEFAULT_START_YEAR = 2000
_DEFAULT_FIRST_YEAR = 1946
_DEFAULT_YEAR = 2024
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
        "--all-seasons",
        action="store_true",
        help="Refresh every season from --start-year back to --first-year.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=_DEFAULT_START_YEAR,
        help=(
            "First season start year to cache when using --all-seasons "
            f"(default: {_DEFAULT_START_YEAR})"
        ),
    )
    parser.add_argument(
        "--first-year",
        type=int,
        default=_DEFAULT_FIRST_YEAR,
        help=(
            "Earliest season start year to cache, inclusive, when using "
            f"--all-seasons (default: {_DEFAULT_FIRST_YEAR})"
        ),
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
    parser.add_argument(
        "--player-metrics-db-path",
        type=Path,
        default=Path("data/app/player_metrics.sqlite3"),
        help="SQLite cache path for normalized team-season rows.",
    )
    parser.add_argument(
        "--failure-log-path",
        type=Path,
        default=DEFAULT_INGEST_FAILURE_LOG_PATH,
        help="JSONL file where ingest failures are appended.",
    )
    return parser


def _get_season_list_from_args(args) -> list[Season]:
    season_type_str = args.season_type or _DEFAULT_SEASON_TYPE
    if args.start_year and args.first_year:
        start_year = args.start_year or _DEFAULT_START_YEAR
        first_year = args.first_year or _DEFAULT_FIRST_YEAR  # todo. rename to end year
    elif args.season:
        start_year = first_year = args.season
    else:
        start_year = first_year = _DEFAULT_YEAR

    return build_season_list(start_year, first_year, season_type_str)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    seasons_list = _get_season_list_from_args(args)
    season_count = len(seasons_list)
    failure_counts: dict[str, int] = {}
    failed_scopes: list[str] = []
    for season_index, season in enumerate(seasons_list, start=1):
        if season_count > 1:
            print(f"[{season_index}/{season_count}] caching {season}")
        teams: list[Team]
        if args.teams:
            teams = [Team.from_abbreviation(team_code, season=season) for team_code in args.teams]
        else:
            teams = Team.all_active_in_season(season)
        team_total = len(teams)
        for team_index, team in enumerate(teams, start=1):
            team_season_scope = f"{team.abbreviation(season=season)} {season}"
            try:
                request = IngestRequest(team, season)
                result = refresh(
                    request,
                    log=filtered_log,
                    progress=lambda payload, team_index=team_index: render_progress_line(
                        team_index,
                        team_total,
                        payload,
                    ),
                )
            except FetchError as exc:
                record_failure(
                    failure_counts,
                    failed_scopes,
                    failure_kind="fetch_error",
                    scope=team_season_scope,
                )
                append_ingest_failure_log(
                    team=team,
                    season=season,
                    failure_kind="fetch_error",
                    error=exc,
                    log_path=args.failure_log_path,
                )
                render_team_fetch_failed_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team,
                    season=season,
                    error_type=exc.last_error_type,
                )
                sys.stdout.write("\n")
                sys.stderr.write(f"Fetch failed for {team} {season}: {exc}\n")
                sys.stderr.flush()
                continue
            except PartialTeamSeasonError as exc:
                record_failure(
                    failure_counts,
                    failed_scopes,
                    failure_kind="partial_scope_error",
                    scope=team_season_scope,
                )
                append_ingest_failure_log(
                    team=team,
                    season=season,
                    failure_kind="partial_scope_error",
                    error=exc,
                    log_path=args.failure_log_path,
                )
                render_team_partial_failed_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team,
                    season=season,
                    failed_games=exc.failed_games,
                    total_games=exc.total_games,
                )
                sys.stdout.write("\n")
                sys.stderr.write(
                    f"Incomplete cache for {team} {season}: "
                    f"{exc.failed_games}/{exc.total_games} games failed normalization\n"
                )
                sys.stderr.write(f"{render_partial_failure_details(exc)}\n")
                sys.stderr.flush()
                continue
            except ValueError as exc:
                reason = str(exc)
                record_failure(
                    failure_counts,
                    failed_scopes,
                    failure_kind="validation_error",
                    scope=team_season_scope,
                )
                append_ingest_failure_log(
                    team=team,
                    season=season,
                    failure_kind="validation_error",
                    error=exc,
                    log_path=args.failure_log_path,
                )
                render_team_validation_failed_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team,
                    season=season,
                    reason=reason,
                )
                sys.stdout.write("\n")
                sys.stderr.write(f"Validation failed for {team} {season}: {reason}\n")
                sys.stderr.flush()
                continue
            render_team_complete_line(team_index, team_total, result)
            sys.stdout.write("\n")
    if failure_counts:
        render_failure_summary(
            failure_counts=failure_counts,
            failed_scopes=failed_scopes,
        )
        return 1
    return 0


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
