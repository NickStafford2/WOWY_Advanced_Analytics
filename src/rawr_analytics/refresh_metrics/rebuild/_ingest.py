from __future__ import annotations

from rawr_analytics.refresh_metrics.rebuild._events import (
    RebuildEventFn,
    RebuildTeamFailureEvent,
)
from rawr_analytics.shared.ingest import FetchError, PartialTeamSeasonError
from rawr_analytics.sources.nba_api.ingest._models import (
    FailureLogFn,
    IngestEvent,
    IngestTeamFailedEvent,
    SeasonRangeFailure,
    SeasonRangeResult,
)
from rawr_analytics.sources.nba_api.ingest.api import refresh_season_range


def refresh_rebuild_ingest(
    *,
    start_year: int,
    end_year: int,
    season_type: str,
    teams: list[str] | None,
    event_fn: RebuildEventFn | None,
    failure_log_fn: FailureLogFn | None,
) -> SeasonRangeResult:
    def _emit_ingest_event(event: IngestEvent) -> None:
        assert event_fn is not None
        forward_rebuild_ingest_event(event_fn, event)

    return refresh_season_range(
        start_year=start_year,
        end_year=end_year,
        season_type=season_type,
        team_abbreviations=teams,
        event_fn=None if event_fn is None else _emit_ingest_event,
        failure_log_fn=failure_log_fn,
    )


def forward_rebuild_ingest_event(
    event_fn: RebuildEventFn,
    event: IngestEvent,
) -> None:
    if isinstance(event, IngestTeamFailedEvent):
        event_fn(
            build_rebuild_team_failure_event(
                team_index=event.team_index,
                team_total=event.team_total,
                failure=event.failure,
            )
        )
        return
    event_fn(event)


def build_rebuild_team_failure_event(
    *,
    team_index: int,
    team_total: int,
    failure: SeasonRangeFailure,
) -> RebuildTeamFailureEvent:
    request = failure.request
    team = request.team
    season = request.season
    error = failure.error
    team_label = team.abbreviation(season=season)
    season_label = str(season)

    if failure.failure_kind == "fetch_error":
        assert isinstance(error, FetchError)
        return RebuildTeamFailureEvent(
            team_index=team_index,
            team_total=team_total,
            scope=failure.scope,
            team=team,
            season=season,
            team_label=team_label,
            season_label=season_label,
            failure_kind=failure.failure_kind,
            error=error,
            fetch_error_type=error.last_error_type,
            failed_games=None,
            total_games=None,
            reason=str(error),
        )

    if failure.failure_kind == "partial_scope_error":
        assert isinstance(error, PartialTeamSeasonError)
        return RebuildTeamFailureEvent(
            team_index=team_index,
            team_total=team_total,
            scope=failure.scope,
            team=team,
            season=season,
            team_label=team_label,
            season_label=season_label,
            failure_kind=failure.failure_kind,
            error=error,
            fetch_error_type=None,
            failed_games=error.failed_games,
            total_games=error.total_games,
            reason=str(error),
            failed_game_details=list(error.failed_game_details),
            failure_reason_counts=dict(sorted(error.failure_reason_counts.items())),
            failure_reason_examples={
                reason: examples[:]
                for reason, examples in sorted(error.failure_reason_examples.items())
            },
        )

    return RebuildTeamFailureEvent(
        team_index=team_index,
        team_total=team_total,
        scope=failure.scope,
        team=team,
        season=season,
        team_label=team_label,
        season_label=season_label,
        failure_kind=failure.failure_kind,
        error=error,
        fetch_error_type=None,
        failed_games=None,
        total_games=None,
        reason=str(error),
    )
