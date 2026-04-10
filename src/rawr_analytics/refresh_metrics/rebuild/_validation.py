from __future__ import annotations

from rawr_analytics.refresh_metrics.rebuild._events import (
    RebuildEventFn,
    RebuildValidationProgressEvent,
)
from rawr_analytics.data import (
    render_rebuild_validation_summary,
    validate_rebuild_storage,
)
from rawr_analytics.data.db_validation import DatabaseValidationSummary


def format_rebuild_validation_summary(
    summary: DatabaseValidationSummary,
    *,
    top_n: int = 10,
) -> str:
    return render_rebuild_validation_summary(summary, top_n=top_n)


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

    return validate_rebuild_storage(
        progress=None if event_fn is None else _emit_validation_progress
    )
