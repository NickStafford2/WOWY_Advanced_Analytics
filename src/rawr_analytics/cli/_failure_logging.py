from __future__ import annotations

from rawr_analytics.refresh_metrics.rebuild import RebuildIngestFailure
from rawr_analytics.services._ingest_failure_log import append_ingest_failure_log


def append_failure_log_entry(entry: RebuildIngestFailure) -> None:
    append_ingest_failure_log(
        team=entry.team,
        season=entry.season,
        failure_kind=entry.failure_kind,
        error=entry.error,
    )
