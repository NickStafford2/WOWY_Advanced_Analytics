from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.shared.common import LogFn
from rawr_analytics.shared.season import Season
from rawr_analytics.workflows import (
    IngestProgress,
    IngestResult,
    SeasonRangeFailure,
    SeasonRangeResult,
)
from rawr_analytics.workflows import refresh_season_range as _refresh_season_range

_SeasonStartedFn = Callable[[int, int, Season], None]
_TeamCompletedFn = Callable[[int, int, IngestResult], None]
_TeamFailedFn = Callable[[int, int, SeasonRangeFailure], None]
_ProgressFn = Callable[[int, int, IngestProgress], None]


@dataclass(frozen=True)
class IngestRefreshRequest:
    start_year: int
    end_year: int
    season_type: str
    season_str: str | None = None
    team_abbreviations: list[str] | None = None


def refresh_season_range(
    request: IngestRefreshRequest,
    *,
    log_fn: LogFn | None = None,
    progress_fn: _ProgressFn | None = None,
    season_started_fn: _SeasonStartedFn | None = None,
    team_completed_fn: _TeamCompletedFn | None = None,
    team_failed_fn: _TeamFailedFn | None = None,
) -> SeasonRangeResult:
    return _refresh_season_range(
        season_str=request.season_str,
        start_year=request.start_year,
        end_year=request.end_year,
        season_type=request.season_type,
        team_abbreviations=request.team_abbreviations,
        log_fn=log_fn,
        progress_fn=progress_fn,
        season_started_fn=season_started_fn,
        team_completed_fn=team_completed_fn,
        team_failed_fn=team_failed_fn,
    )


__all__ = [
    "IngestRefreshRequest",
    "IngestResult",
    "SeasonRangeFailure",
    "SeasonRangeResult",
    "refresh_season_range",
]
