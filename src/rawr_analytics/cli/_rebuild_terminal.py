from __future__ import annotations

import sys

from rawr_analytics.cli._ingest_terminal import (
    format_partial_failure_details,
    render_progress_line,
    render_team_complete_line,
    render_team_fetch_failed_line,
    render_team_partial_failed_line,
    render_team_validation_failed_line,
)
from rawr_analytics.nba import append_ingest_failure_log
from rawr_analytics.progress import TerminalProgressBar
from rawr_analytics.services import (
    IngestSeasonStartedEvent,
    IngestTeamCompletedEvent,
    IngestTeamProgressEvent,
    RebuildEvent,
    RebuildMetricRefreshProgressEvent,
    RebuildTeamFailureEvent,
    RebuildValidationProgressEvent,
)

_METRIC_PROGRESS_BARS: dict[str, TerminalProgressBar] = {}
_VALIDATION_PROGRESS_BAR: TerminalProgressBar | None = None


def render_rebuild_event(event: RebuildEvent) -> None:
    if isinstance(event, IngestSeasonStartedEvent):
        if event.season_total > 1:
            print(f"[{event.season_index}/{event.season_total}] caching {event.season}")
        return
    if isinstance(event, IngestTeamProgressEvent):
        render_progress_line(event.team_index, event.team_total, event.progress)
        return
    if isinstance(event, IngestTeamCompletedEvent):
        render_team_complete_line(event.team_index, event.team_total, event.result)
        sys.stdout.write("\n")
        return
    if isinstance(event, RebuildTeamFailureEvent):
        render_rebuild_team_failure(event)
        return
    if isinstance(event, RebuildMetricRefreshProgressEvent):
        render_metric_progress(
            event.metric,
            event.current,
            event.total,
            event.detail,
        )
        return
    if isinstance(event, RebuildValidationProgressEvent):
        render_validation_progress(event.current, event.total, event.label)


def render_rebuild_team_failure(event: RebuildTeamFailureEvent) -> None:
    append_ingest_failure_log(
        team=event.team,
        season=event.season,
        failure_kind=event.failure_kind,
        error=event.error,
    )

    if event.failure_kind == "fetch_error":
        render_team_fetch_failed_line(
            team_index=event.team_index,
            team_total=event.team_total,
            team_label=event.team_label,
            season_label=event.season_label,
            error_type=event.fetch_error_type or "unknown",
        )
        sys.stdout.write("\n")
        sys.stderr.write(f"Fetch failed for {event.scope}: {event.reason}\n")
        sys.stderr.flush()
        return

    if event.failure_kind == "partial_scope_error":
        render_team_partial_failed_line(
            team_index=event.team_index,
            team_total=event.team_total,
            team_label=event.team_label,
            season_label=event.season_label,
            failed_games=event.failed_games or 0,
            total_games=event.total_games or 0,
        )
        sys.stdout.write("\n")
        sys.stderr.write(
            f"Incomplete cache for {event.scope}: "
            f"{event.failed_games or 0}/{event.total_games or 0} games failed normalization\n"
        )
        if event.failed_game_details is not None and event.failure_reason_counts is not None:
            sys.stderr.write(
                f"{format_partial_failure_details(
                    failed_game_details=event.failed_game_details,
                    failure_reason_counts=event.failure_reason_counts,
                    failure_reason_examples=event.failure_reason_examples or {},
                )}\n"
            )
        sys.stderr.flush()
        return

    render_team_validation_failed_line(
        team_index=event.team_index,
        team_total=event.team_total,
        team_label=event.team_label,
        season_label=event.season_label,
        reason=event.reason,
    )
    sys.stdout.write("\n")
    sys.stderr.write(f"Validation failed for {event.scope}: {event.reason}\n")
    sys.stderr.flush()


def render_metric_progress(metric: str, current: int, total: int, detail: str) -> None:
    progress_bar = _METRIC_PROGRESS_BARS.get(metric)
    if progress_bar is None:
        progress_bar = TerminalProgressBar(f"Refresh {metric}", total=max(total, 1))
        _METRIC_PROGRESS_BARS[metric] = progress_bar
    progress_bar.total = max(total, 1)
    progress_bar.update(current, detail=detail)
    if current >= total:
        progress_bar.finish(detail="done")
        del _METRIC_PROGRESS_BARS[metric]


def render_validation_progress(current: int, total: int, label: str) -> None:
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
