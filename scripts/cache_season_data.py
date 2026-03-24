from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from wowy.nba.ingest import (
    DEFAULT_SOURCE_DATA_DIR,
    cache_team_season_data,
)
from wowy.nba.errors import (
    FetchError,
    GameNormalizationFailure,
    PartialTeamSeasonError,
    TeamSeasonConsistencyError,
)
from wowy.nba.ingest_logging import (
    DEFAULT_INGEST_FAILURE_LOG_PATH,
    append_ingest_failure_log,
)
from wowy.nba.seasons import canonicalize_season_string
from wowy.nba.season_types import canonicalize_season_type
from wowy.nba.team_identity import (
    list_expected_team_abbreviations_for_season,
    team_is_active_for_season,
)


_LAST_STATUS_LINE_LENGTH = 0
DEFAULT_START_YEAR = 2024
DEFAULT_FIRST_YEAR = 1946


def build_parser() -> argparse.ArgumentParser:
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
        default=DEFAULT_START_YEAR,
        help=f"First season start year to cache when using --all-seasons (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--first-year",
        type=int,
        default=DEFAULT_FIRST_YEAR,
        help=(
            "Earliest season start year to cache, inclusive, when using "
            f"--all-seasons (default: {DEFAULT_FIRST_YEAR})"
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


def season_string(start_year: int) -> str:
    end_year = (start_year + 1) % 100
    return f"{start_year}-{end_year:02d}"


def build_season_strings(start_year: int, first_year: int) -> list[str]:
    if start_year < first_year:
        raise ValueError("Start year must be greater than or equal to first year")
    return [season_string(year) for year in range(start_year, first_year - 1, -1)]


def resolve_teams(team_codes: list[str] | None, season: str) -> list[str]:
    if team_codes:
        return [team_code.upper() for team_code in team_codes]
    return list_expected_team_abbreviations_for_season(season)


def render_progress_line(
    team_index: int,
    team_total: int,
    payload: dict,
) -> None:
    current = payload["current"]
    total = payload["total"]
    status = payload["status"]
    team = payload["team"]
    season = payload["season"]
    game_id = payload["game_id"]
    filled = 20 if total == 0 else int((current / total) * 20)
    bar = "#" * filled + "-" * (20 - filled)
    line = (
        f"  [{team_index:>2}/{team_total}] {team} {season} "
        f"{current}/{total} [{bar}] {status:<7} {game_id}"
    )
    write_status_line(line)


def filtered_log(message: str) -> None:
    if not _should_emit_log_message(message):
        return
    sys.stderr.write(f"{message}\n")
    sys.stderr.flush()


def _should_emit_log_message(message: str) -> bool:
    return message.startswith("cache discard") or message.startswith("cache skip")


def write_status_line(line: str) -> None:
    global _LAST_STATUS_LINE_LENGTH
    padding = max(0, _LAST_STATUS_LINE_LENGTH - len(line))
    sys.stdout.write(f"\r{line}{' ' * padding}")
    sys.stdout.flush()
    _LAST_STATUS_LINE_LENGTH = len(line)


def render_team_complete_line(
    team_index: int,
    team_total: int,
    summary,
) -> None:
    line = (
        f"  [{team_index:>2}/{team_total}] {summary.team} {summary.season} "
        f"{summary.processed_games}/{summary.total_games} "
        f"league={'cached' if summary.league_games_source == 'cached' else 'fetched'} "
        f"boxscores={summary.fetched_box_scores} fetched, {summary.cached_box_scores} cached "
        f"skipped={summary.skipped_games}"
    )
    write_status_line(line)


def render_team_failed_line(
    team_index: int,
    team_total: int,
    team: str,
    season: str,
    reason: str,
) -> None:
    line = f"  [{team_index:>2}/{team_total}] {team} {season} failed consistency={reason}"
    write_status_line(line)


def render_team_partial_failed_line(
    team_index: int,
    team_total: int,
    team: str,
    season: str,
    failed_games: int,
    total_games: int,
) -> None:
    line = (
        f"  [{team_index:>2}/{team_total}] {team} {season} "
        f"failed partial={failed_games}/{total_games}"
    )
    write_status_line(line)


def render_partial_failure_details(error: PartialTeamSeasonError) -> str:
    lines = ["Failure reasons:"]
    details_by_game_id = {
        failure.game_id: failure for failure in error.failed_game_details
    }
    ranked_reasons = sorted(
        error.failure_reason_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )
    for reason, count in ranked_reasons:
        example_game_ids = error.failure_reason_examples.get(reason, [])[:3]
        lines.append(f"  - {count} games: {reason}")
        for game_id in example_game_ids:
            failure = details_by_game_id.get(game_id)
            if failure is None:
                lines.append(f"    {game_id}: details unavailable")
                continue
            detail = _summarize_game_failure_detail(failure)
            lines.append(f"    {game_id}: {detail}")
    return "\n".join(lines)


def _summarize_game_failure_detail(failure: GameNormalizationFailure) -> str:
    message = failure.message
    if "; nba_api_" not in message:
        return message

    summary, raw_payload = message.split("; nba_api_", maxsplit=1)
    raw_json = raw_payload.split("=", maxsplit=1)[-1].strip()
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        return summary

    parts: list[str] = []
    player_name = str(payload.get("PLAYER_NAME", "")).strip()
    if player_name:
        parts.append(f"player={player_name!r}")
    min_value = payload.get("MIN")
    if min_value is not None or "MIN" in payload:
        parts.append(f"min={min_value!r}")
    comment = str(payload.get("COMMENT", "")).strip()
    if comment:
        parts.append(f"comment={comment!r}")

    for key in ("TEAM_ABBREVIATION", "TEAM_ID"):
        value = payload.get(key)
        if value is not None and value != "":
            parts.append(f"{key.lower()}={value!r}")
            break

    if not parts:
        return summary
    return f"{summary} ({', '.join(parts)})"


def render_team_validation_failed_line(
    team_index: int,
    team_total: int,
    team: str,
    season: str,
) -> None:
    line = f"  [{team_index:>2}/{team_total}] {team} {season} failed validation"
    write_status_line(line)


def render_team_fetch_failed_line(
    team_index: int,
    team_total: int,
    team: str,
    season: str,
    error_type: str,
) -> None:
    line = (
        f"  [{team_index:>2}/{team_total}] {team} {season} "
        f"failed fetch={error_type}"
    )
    write_status_line(line)


def render_team_skipped_line(
    team_index: int,
    team_total: int,
    team: str,
    season: str,
    reason: str,
) -> None:
    line = f"  [{team_index:>2}/{team_total}] {team} {season} skipped {reason}"
    write_status_line(line)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    season_type = canonicalize_season_type(args.season_type)
    if args.all_seasons or not args.season:
        seasons = build_season_strings(args.start_year, args.first_year)
    else:
        seasons = [canonicalize_season_string(args.season)]

    season_count = len(seasons)
    failure_counts: dict[str, int] = {}
    failed_scopes: list[str] = []
    for season_index, season in enumerate(seasons, start=1):
        if season_count > 1:
            print(f"[{season_index}/{season_count}] caching {season}")
        team_codes = resolve_teams(args.teams, season)
        team_total = len(team_codes)
        for team_index, team_code in enumerate(team_codes, start=1):
            team_season_scope = f"{team_code} {season}"
            if not team_is_active_for_season(team_code, season):
                render_team_skipped_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team_code,
                    season=season,
                    reason="not-active-in-season",
                )
                sys.stdout.write("\n")
                continue
            try:
                summary = cache_team_season_data(
                    team_abbreviation=team_code,
                    season=season,
                    season_type=season_type,
                    source_data_dir=DEFAULT_SOURCE_DATA_DIR,
                    player_metrics_db_path=args.player_metrics_db_path,
                    log=filtered_log,
                    progress=lambda payload, team_index=team_index: render_progress_line(
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
                render_team_fetch_failed_line(
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
            except TeamSeasonConsistencyError as exc:
                _record_failure(
                    failure_counts,
                    failed_scopes,
                    failure_kind="consistency_error",
                    scope=team_season_scope,
                )
                append_ingest_failure_log(
                    team=team_code,
                    season=season,
                    season_type=season_type,
                    failure_kind="consistency_error",
                    error=exc,
                    log_path=args.failure_log_path,
                )
                render_team_failed_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team_code,
                    season=season,
                    reason=exc.reason,
                )
                sys.stdout.write("\n")
                sys.stderr.write(
                    f"Inconsistent cache for {team_code} {season}: {exc.reason}\n"
                )
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
                render_team_partial_failed_line(
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
                sys.stderr.write(f"{render_partial_failure_details(exc)}\n")
                sys.stderr.flush()
                continue
            except ValueError as exc:
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
                render_team_validation_failed_line(
                    team_index=team_index,
                    team_total=team_total,
                    team=team_code,
                    season=season,
                )
                sys.stdout.write("\n")
                sys.stderr.write(
                    f"Validation failed for {team_code} {season}: {exc}\n"
                )
                sys.stderr.flush()
                continue
            render_team_complete_line(team_index, team_total, summary)
            sys.stdout.write("\n")
    if failure_counts:
        _render_failure_summary(
            failure_counts=failure_counts,
            failed_scopes=failed_scopes,
        )
        return 1
    return 0


def _record_failure(
    failure_counts: dict[str, int],
    failed_scopes: list[str],
    *,
    failure_kind: str,
    scope: str,
) -> None:
    failure_counts[failure_kind] = failure_counts.get(failure_kind, 0) + 1
    failed_scopes.append(scope)


def _render_failure_summary(
    *,
    failure_counts: dict[str, int],
    failed_scopes: list[str],
) -> None:
    summary = ", ".join(
        f"{kind}={count}" for kind, count in sorted(failure_counts.items())
    )
    scope_preview = ", ".join(failed_scopes[:10])
    suffix = "" if len(failed_scopes) <= 10 else ", ..."
    sys.stderr.write(
        f"Completed with failures across {len(failed_scopes)} team-seasons: {summary}\n"
    )
    sys.stderr.write(f"Failed scopes: {scope_preview}{suffix}\n")
    sys.stderr.flush()


def run(argv: list[str] | None = None) -> int:
    try:
        return main(argv)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted. Shutting down cleanly.\n")
        sys.stderr.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
