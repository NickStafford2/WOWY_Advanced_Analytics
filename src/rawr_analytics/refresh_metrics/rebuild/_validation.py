from __future__ import annotations

from collections.abc import Callable

from rawr_analytics.data.audit.audit import audit_player_metrics_db
from rawr_analytics.data.audit.reporting import (
    DatabaseValidationSummary,
    render_validation_summary,
    summarize_validation_report,
)
from rawr_analytics.refresh_metrics.rebuild._events import (
    RebuildEventFn,
    RebuildValidationProgressEvent,
)

ValidationProgressFn = Callable[[int, int, str], None]


def format_rebuild_validation_summary(
    summary: DatabaseValidationSummary,
    *,
    top_n: int = 10,
) -> str:
    return render_validation_summary(summary, top_n=top_n)


def validate_rebuild_result(
    *,
    event_fn: RebuildEventFn | None,
) -> DatabaseValidationSummary:
    def _emit_validation_progress(current: int, total: int, label: str) -> None:
        assert event_fn is not None
        event_fn(
            RebuildValidationProgressEvent(
                current=current,
                total=total,
                label=label,
            )
        )

    progress = None if event_fn is None else _emit_validation_progress
    report = audit_player_metrics_db(progress=progress)
    return summarize_validation_report(report)
