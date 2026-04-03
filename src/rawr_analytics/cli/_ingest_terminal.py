from __future__ import annotations

import json
import sys

from rawr_analytics.nba import FetchError, PartialTeamSeasonError
from rawr_analytics.nba.errors import GameNormalizationFailure
from rawr_analytics.services import (
    IngestEvent,
    IngestProgress,
    IngestResult,
    IngestSeasonStartedEvent,
    IngestTeamCompletedEvent,
    IngestTeamFailedEvent,
    IngestTeamProgressEvent,
    SeasonRangeFailure,
)

_LAST_STATUS_LINE_LENGTH = 0


def filtered_log(message: str) -> None:
    if not _should_emit_log_message(message):
        return
    sys.stderr.write(f"{message}\n")
    sys.stderr.flush()


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


def render_ingest_failure(
    team_index: int,
    team_total: int,
    failure: SeasonRangeFailure,
) -> None:
    request = failure.request
    team = request.team
    season = request.season
    error = failure.error

    if failure.failure_kind == "fetch_error":
        assert isinstance(error, FetchError)
        render_team_fetch_failed_line(
            team_index=team_index,
            team_total=team_total,
            team_label=team.abbreviation(season=season),
            season_label=str(season),
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
            team_label=team.abbreviation(season=season),
            season_label=str(season),
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

    render_team_validation_failed_line(
        team_index=team_index,
        team_total=team_total,
        team_label=team.abbreviation(season=season),
        season_label=str(season),
        reason=str(error),
    )
    sys.stdout.write("\n")
    sys.stderr.write(f"Validation failed for {request.label}: {error}\n")
    sys.stderr.flush()


def render_ingest_event(event: IngestEvent) -> None:
    if isinstance(event, IngestSeasonStartedEvent):
        _render_season_started(event)
        return
    if isinstance(event, IngestTeamProgressEvent):
        render_progress_line(event.team_index, event.team_total, event.progress)
        return
    if isinstance(event, IngestTeamCompletedEvent):
        render_team_complete_line(event.team_index, event.team_total, event.result)
        sys.stdout.write("\n")
        return
    if isinstance(event, IngestTeamFailedEvent):
        render_ingest_failure(event.team_index, event.team_total, event.failure)
        return


def _render_season_started(event: IngestSeasonStartedEvent) -> None:
    if event.season_total > 1:
        print(f"[{event.season_index}/{event.season_total}] caching {event.season}")


def render_partial_failure_details(error: PartialTeamSeasonError) -> str:
    return format_partial_failure_details(
        failed_game_details=list(error.failed_game_details),
        failure_reason_counts=dict(error.failure_reason_counts),
        failure_reason_examples={
            reason: examples[:]
            for reason, examples in error.failure_reason_examples.items()
        },
    )


def format_partial_failure_details(
    *,
    failed_game_details: list[GameNormalizationFailure],
    failure_reason_counts: dict[str, int],
    failure_reason_examples: dict[str, list[str]],
) -> str:
    lines = ["Failure reasons:"]
    details_by_game_id = {failure.game_id: failure for failure in failed_game_details}
    ranked_reasons = sorted(
        failure_reason_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )
    for reason, count in ranked_reasons:
        example_game_ids = failure_reason_examples.get(reason, [])[:3]
        lines.append(f"  - {count} games: {reason}")
        for game_id in example_game_ids:
            failure = details_by_game_id.get(game_id)
            if failure is None:
                lines.append(f"    {game_id}: details unavailable")
                continue
            lines.append(f"    {game_id}: {_summarize_game_failure_detail(failure)}")
    return "\n".join(lines)


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


def render_team_complete_line(
    team_index: int,
    team_total: int,
    result: IngestResult,
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


def _should_emit_log_message(message: str) -> bool:
    return message.startswith("cache discard") or message.startswith("cache skip")


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


def _write_status_line(line: str) -> None:
    global _LAST_STATUS_LINE_LENGTH
    padding = max(0, _LAST_STATUS_LINE_LENGTH - len(line))
    sys.stdout.write(f"\r{line}{' ' * padding}")
    sys.stdout.flush()
    _LAST_STATUS_LINE_LENGTH = len(line)
