from __future__ import annotations

from rawr_analytics.refresh_metrics.rebuild._events import (
    RebuildEventFn,
    RebuildFailureLogFn,
    RebuildIngestFailure,
    RebuildIngestResult,
    RebuildSeasonStartedEvent,
    RebuildTeamCompletedEvent,
    RebuildTeamFailureEvent,
    RebuildTeamProgressEvent,
)
from rawr_analytics.shared.ingest import FetchError, PartialTeamSeasonError
from rawr_analytics.sources.nba_api.ingest._models import (
    IngestEvent,
    IngestSeasonStartedEvent,
    IngestTeamCompletedEvent,
    IngestTeamFailedEvent,
    IngestTeamProgressEvent,
    SeasonRangeFailure,
)
from rawr_analytics.sources.nba_api.ingest.api import refresh_season_range


def run_ingest(
    *,
    start_year: int,
    end_year: int,
    season_type: str,
    teams: list[str] | None,
    event_fn: RebuildEventFn | None,
    failure_log_fn: RebuildFailureLogFn | None,
) -> RebuildIngestResult:
    def _emit_ingest_event(event: IngestEvent) -> None:
        assert event_fn is not None
        _forward_emit_ingest_event(event_fn, event)

    source_result = refresh_season_range(
        start_year=start_year,
        end_year=end_year,
        season_type=season_type,
        team_abbreviations=teams,
        event_fn=None if event_fn is None else _emit_ingest_event,
        failure_log_fn=_build_failure_log_fn(failure_log_fn),
    )
    return RebuildIngestResult(
        attempted_team_seasons=source_result.attempted_team_seasons,
        completed_team_seasons=source_result.completed_team_seasons,
        failures=[_build_rebuild_ingest_failure(failure) for failure in source_result.failures],
    )


def _forward_emit_ingest_event(
    event_fn: RebuildEventFn,
    event: IngestEvent,
) -> None:
    if isinstance(event, IngestSeasonStartedEvent):
        event_fn(
            RebuildSeasonStartedEvent(
                season_index=event.season_index,
                season_total=event.season_total,
                season_label=str(event.season),
            )
        )
        return
    if isinstance(event, IngestTeamProgressEvent):
        event_fn(
            RebuildTeamProgressEvent(
                team_index=event.team_index,
                team_total=event.team_total,
                progress=event.progress,
            )
        )
        return
    if isinstance(event, IngestTeamCompletedEvent):
        result = event.result
        request = result.request
        summary = result.summary
        event_fn(
            RebuildTeamCompletedEvent(
                team_index=event.team_index,
                team_total=event.team_total,
                team_label=request.team.abbreviation(season=request.season),
                season_label=str(request.season),
                processed_games=summary.processed_games,
                total_games=summary.total_games,
                league_games_source=summary.league_games_source,
                fetched_box_scores=summary.fetched_box_scores,
                cached_box_scores=summary.cached_box_scores,
            )
        )
        return
    if isinstance(event, IngestTeamFailedEvent):
        event_fn(
            _build_team_failure_event(
                team_index=event.team_index,
                team_total=event.team_total,
                failure=event.failure,
            )
        )
        return


def _build_team_failure_event(
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


def _build_failure_log_fn(
    failure_log_fn: RebuildFailureLogFn | None,
):
    if failure_log_fn is None:
        return None

    def _append_failure_log(failure: SeasonRangeFailure) -> None:
        failure_log_fn(_build_rebuild_ingest_failure(failure))

    return _append_failure_log


def _build_rebuild_ingest_failure(failure: SeasonRangeFailure) -> RebuildIngestFailure:
    request = failure.request
    return RebuildIngestFailure(
        team=request.team,
        season=request.season,
        failure_kind=failure.failure_kind,
        error=failure.error,
    )
