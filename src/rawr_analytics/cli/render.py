from __future__ import annotations

import sys

from rawr_analytics.services import IngestProgress

_LAST_STATUS_LINE_LENGTH = 0


def render_progress_line(team_index: int, team_total: int, progress: IngestProgress) -> None:
    current = progress.current
    total = progress.total
    status = progress.status
    team_id = progress.team.team_id
    season = progress.season.id
    game_id = progress.game_id or ""
    filled = 20 if total == 0 else int((current / total) * 20)
    bar = "#" * filled + "-" * (20 - filled)
    line = (
        f"  [{team_index:>2}/{team_total}] {team_id} {season} "
        f"{current}/{total} [{bar}] {status:<7} {game_id}"
    )
    _write_status_line(line)


def filtered_log(message: str) -> None:
    if not _should_emit_log_message(message):
        return
    sys.stderr.write(f"{message}\n")
    sys.stderr.flush()


def _should_emit_log_message(message: str) -> bool:
    return message.startswith("cache discard") or message.startswith("cache skip")


def _write_status_line(line: str) -> None:
    global _LAST_STATUS_LINE_LENGTH
    padding = max(0, _LAST_STATUS_LINE_LENGTH - len(line))
    sys.stdout.write(f"\r{line}{' ' * padding}")
    sys.stdout.flush()
    _LAST_STATUS_LINE_LENGTH = len(line)


def render_team_complete_line(
    team_index: int,
    team_total: int,
    result,
) -> None:
    request = result.request
    summary = result.summary
    line = (
        f"  [{team_index:>2}/{team_total}] "
        f"{request.team.abbreviation(season=request.season)} {request.season} "
        f"{summary.processed_games}/{summary.total_games} "
        f"league={'cached' if summary.league_games_source == 'cached' else 'fetched'} "
        f"boxscores={summary.fetched_box_scores} fetched, {summary.cached_box_scores} cached "
    )
    _write_status_line(line)


def render_team_partial_failed_line(
    team_index: int,
    team_total: int,
    team_label: str,
    season_label: str,
    failed_games: int,
    total_games: int,
) -> None:
    line = (
        f"  [{team_index:>2}/{team_total}] {team_label} {season_label} "
        f"failed partial={failed_games}/{total_games}"
    )
    _write_status_line(line)


def render_team_validation_failed_line(
    team_index: int,
    team_total: int,
    team_label: str,
    season_label: str,
    reason: str,
) -> None:
    line = (
        f"  [{team_index:>2}/{team_total}] "
        f"{team_label} {season_label} failed validation={reason}"
    )
    _write_status_line(line)


def render_team_fetch_failed_line(
    team_index: int,
    team_total: int,
    team_label: str,
    season_label: str,
    error_type: str,
) -> None:
    line = (
        f"  [{team_index:>2}/{team_total}] "
        f"{team_label} {season_label} failed fetch={error_type}"
    )
    _write_status_line(line)


def _render_team_skipped_line(
    team_index: int,
    team_total: int,
    team_label: str,
    season_label: str,
    reason: str,
) -> None:
    line = f"  [{team_index:>2}/{team_total}] {team_label} {season_label} skipped {reason}"
    _write_status_line(line)


def record_failure(
    failure_counts: dict[str, int],
    failed_scopes: list[str],
    *,
    failure_kind: str,
    scope: str,
) -> None:
    failure_counts[failure_kind] = failure_counts.get(failure_kind, 0) + 1
    failed_scopes.append(scope)


def render_failure_summary(
    *,
    failure_counts: dict[str, int],
    failed_scopes: list[str],
) -> None:
    total_failures = len(failed_scopes)
    summary = ", ".join(f"{kind}={count}" for kind, count in sorted(failure_counts.items()))
    scope_preview = ", ".join(failed_scopes[:10])
    suffix = "" if len(failed_scopes) <= 10 else ", ..."
    banner = "!" * 72
    sys.stderr.write(f"{banner}\n")
    sys.stderr.write(f"ERROR: season cache finished with {total_failures} failed team-seasons\n")
    sys.stderr.write(f"{banner}\n")
    sys.stderr.write(f"Completed with failures across {total_failures} team-seasons: {summary}\n")
    sys.stderr.write(f"Failed scopes: {scope_preview}{suffix}\n")
    sys.stderr.flush()
