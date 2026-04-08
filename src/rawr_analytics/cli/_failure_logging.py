from __future__ import annotations

from rawr_analytics.services._ingest_failure_log import append_ingest_failure_log
from rawr_analytics.sources.nba_api.ingest._models import IngestFailureLogEntry


def append_failure_log_entry(entry: IngestFailureLogEntry) -> None:
    append_ingest_failure_log(
        team=entry.team,
        season=entry.season,
        failure_kind=entry.failure_kind,
        error=entry.error,
    )
