"""Public application service interface for CLI and web adapters."""

from rawr_analytics.ingest.nba_api import (
    FailureLogFn,
    IngestEvent,
    IngestFailureLogEntry,
    IngestProgress,
    IngestResult,
    IngestSeasonStartedEvent,
    IngestTeamCompletedEvent,
    IngestTeamFailedEvent,
    IngestTeamProgressEvent,
    SeasonRangeFailure,
    SeasonRangeResult,
)

__all__ = [
    "FailureLogFn",
    "IngestEvent",
    "IngestFailureLogEntry",
    "IngestProgress",
    "IngestResult",
    "IngestSeasonStartedEvent",
    "IngestTeamCompletedEvent",
    "IngestTeamFailedEvent",
    "IngestTeamProgressEvent",
    "SeasonRangeFailure",
    "SeasonRangeResult",
]
