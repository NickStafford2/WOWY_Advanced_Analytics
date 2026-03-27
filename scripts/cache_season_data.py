from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from rawr_analytics.nba.errors import (
    FetchError,
    GameNormalizationFailure,
    PartialTeamSeasonError,
)
from rawr_analytics.nba.ingest_logging import (
    DEFAULT_INGEST_FAILURE_LOG_PATH,
    append_ingest_failure_log,
)
from rawr_analytics.nba.team_identity import (
    list_expected_team_abbreviations_for_season,
    team_is_active_for_season,
)
from rawr_analytics.shared.season import Season, build_season_list

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


def _resolve_teams(team_codes: list[str] | None, season: Season) -> list[str]:
    if team_codes:
        return [team_code.upper() for team_code in team_codes]
    return list_expected_team_abbreviations_for_season(season)


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
        team_codes: list[str] = _resolve_teams(args.teams, season)
        team_total = len(team_codes)
        for team_index, team_code in enumerate(team_codes, start=1):
            team_season_scope = f"{team_code} {season}"
            if not team_is_active_for_season(team_code, season):
                _render_team_skipped_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team_code,
                    season=season,
                    reason="not-active-in-season",
                )
                sys.stdout.write("\n")
                continue
            try:
                summary = refresh_normalized_team_season_cache(
                    team_abbreviation=team_code,
                    season_str=season,
                    season_type=season_type,
                    log=_filtered_log,
                    progress_fn=lambda payload, team_index=team_index: _render_progress_line(
                        team_index,
                        team_total,
                        payload,
                    ),
                )
            except FetchError as exc:
                _record_failure(
                    failure_counts,
                    failed_scopes,
                    failure_kind="fetch_error",
                    scope=team_season_scope,
                )
                append_ingest_failure_log(
                    team=team_code,
                    season=season,
                    season_type=season_type,
                    failure_kind="fetch_error",
                    error=exc,
                    log_path=args.failure_log_path,
                )
                _render_team_fetch_failed_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team_code,
                    season=season,
                    error_type=exc.last_error_type,
                )
                sys.stdout.write("\n")
                sys.stderr.write(f"Fetch failed for {team_code} {season}: {exc}\n")
                sys.stderr.flush()
                continue
            except PartialTeamSeasonError as exc:
                _record_failure(
                    failure_counts,
                    failed_scopes,
                    failure_kind="partial_scope_error",
                    scope=team_season_scope,
                )
                append_ingest_failure_log(
                    team=team_code,
                    season=season,
                    season_type=season_type,
                    failure_kind="partial_scope_error",
                    error=exc,
                    log_path=args.failure_log_path,
                )
                _render_team_partial_failed_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team_code,
                    season=season,
                    failed_games=exc.failed_games,
                    total_games=exc.total_games,
                )
                sys.stdout.write("\n")
                sys.stderr.write(
                    f"Incomplete cache for {team_code} {season}: "
                    f"{exc.failed_games}/{exc.total_games} games failed normalization\n"
                )
                sys.stderr.write(f"{_render_partial_failure_details(exc)}\n")
                sys.stderr.flush()
                continue
            except ValueError as exc:
                reason = str(exc)
                _record_failure(
                    failure_counts,
                    failed_scopes,
                    failure_kind="validation_error",
                    scope=team_season_scope,
                )
                append_ingest_failure_log(
                    team=team_code,
                    season=season,
                    season_type=season_type,
                    failure_kind="validation_error",
                    error=exc,
                    log_path=args.failure_log_path,
                )
                _render_team_validation_failed_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team_code,
                    season=season,
                    reason=reason,
                )
                sys.stdout.write("\n")
                sys.stderr.write(f"Validation failed for {team_code} {season}: {reason}\n")
                sys.stderr.flush()
                continue
            _render_team_complete_line(team_index, team_total, summary)
            sys.stdout.write("\n")
    if failure_counts:
        _render_failure_summary(
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
