from __future__ import annotations

from collections.abc import Callable

from rawr_analytics.data._paths import (
    LEGACY_MIXED_DATA_DB_PATH,
    METRIC_STORE_DB_PATH,
    NORMALIZED_CACHE_DB_PATH,
)
from rawr_analytics.data.db_validation import (
    DatabaseValidationSummary,
    audit_player_metrics_db,
    render_validation_summary,
    summarize_validation_report,
)

ValidationProgressFn = Callable[[int, int, str], None]


def prepare_rebuild_storage(*, keep_existing_db: bool) -> bool:
    deleted_existing_db = False
    if not keep_existing_db:
        for db_path in (
            NORMALIZED_CACHE_DB_PATH,
            METRIC_STORE_DB_PATH,
            LEGACY_MIXED_DATA_DB_PATH,
        ):
            if db_path.exists():
                db_path.unlink()
                deleted_existing_db = True
    NORMALIZED_CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return deleted_existing_db


def validate_rebuild_storage(
    *,
    progress: ValidationProgressFn | None = None,
) -> DatabaseValidationSummary:
    return summarize_validation_report(audit_player_metrics_db(progress=progress))


def render_rebuild_validation_summary(
    summary: DatabaseValidationSummary,
    *,
    top_n: int = 10,
) -> str:
    return render_validation_summary(summary, top_n=top_n)


__all__ = [
    "ValidationProgressFn",
    "prepare_rebuild_storage",
    "render_rebuild_validation_summary",
    "validate_rebuild_storage",
]
