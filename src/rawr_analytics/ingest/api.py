from __future__ import annotations

from collections.abc import Callable

from rawr_analytics.ingest._models import (
    FailureLogFn,
    IngestEvent,
    IngestEventFn,
    IngestFailureLogEntry,
    IngestProgress,
    IngestRequest,
    IngestResult,
    IngestSeasonStartedEvent,
    IngestSummary,
    IngestTeamCompletedEvent,
    IngestTeamFailedEvent,
    IngestTeamProgressEvent,
    SeasonRangeFailure,
    SeasonRangeResult,
    _TeamProgressFn,
)
from rawr_analytics.ingest._store import store_team_season
from rawr_analytics.ingest._validation import validate_normalized_team_season_batch
from rawr_analytics.shared.common import LogFn
from rawr_analytics.shared.ingest import FetchError, PartialTeamSeasonError
from rawr_analytics.shared.season import Season, build_season_list
from rawr_analytics.shared.team import Team
from rawr_analytics.sources.nba_api import NbaApiGameIngestUpdate, ingest_team_season


def refresh_team_season(
    request: IngestRequest,
    *,
    log: LogFn | None = print,
    progress: _TeamProgressFn | None = None,
) -> IngestResult:
    source_data = ingest_team_season(
        team=request.team,
        season=request.season,
        log_fn=log,
        update_fn=_build_source_update_fn(request=request, progress_fn=progress),
    )
    result = IngestResult(
        request=request,
        games=source_data.games,
        game_players=source_data.game_players,
        summary=IngestSummary(
            total_games=source_data.total_games,
            processed_games=len(source_data.games),
            fetched_box_scores=source_data.fetched_box_scores,
            cached_box_scores=source_data.cached_box_scores,
            league_games_source=source_data.league_games_source,
        ),
    )
    validate_normalized_team_season_batch(result.to_batch())
    store_team_season(result)
    return result


def refresh_season_range(
    *,
    start_year: int,
    end_year: int,
    season_type: str,
    season_str: str | None = None,
    team_abbreviations: list[str] | None = None,
    log_fn: LogFn | None = None,
    event_fn: IngestEventFn | None = None,
    failure_log_fn: FailureLogFn | None = None,
) -> SeasonRangeResult:
    seasons = _build_seasons(
        start_year=start_year,
        end_year=end_year,
        season_type=season_type,
        season_str=season_str,
    )
    season_total = len(seasons)
    attempted_team_seasons = 0
    completed_team_seasons = 0
    failures: list[SeasonRangeFailure] = []

    for season_index, season in enumerate(seasons, start=1):
        _emit_event(
            event_fn,
            IngestSeasonStartedEvent(
                season_index=season_index,
                season_total=season_total,
                season=season,
            ),
        )

        teams = _resolve_teams(team_abbreviations=team_abbreviations, season=season)
        team_total = len(teams)
        for team_index, team in enumerate(teams, start=1):
            attempted_team_seasons += 1
            team_request = IngestRequest(team=team, season=season)
            try:
                result = refresh_team_season(
                    team_request,
                    log=log_fn,
                    progress=_build_progress_fn(event_fn, team_index, team_total),
                )
            except FetchError as exc:
                failure = SeasonRangeFailure(
                    request=team_request,
                    failure_kind="fetch_error",
                    error=exc,
                )
            except PartialTeamSeasonError as exc:
                failure = SeasonRangeFailure(
                    request=team_request,
                    failure_kind="partial_scope_error",
                    error=exc,
                )
            except ValueError as exc:
                failure = SeasonRangeFailure(
                    request=team_request,
                    failure_kind="validation_error",
                    error=exc,
                )
            else:
                completed_team_seasons += 1
                _emit_event(
                    event_fn,
                    IngestTeamCompletedEvent(
                        team_index=team_index,
                        team_total=team_total,
                        result=result,
                    ),
                )
                continue

            failures.append(failure)
            _append_failure_log(failure_log_fn, failure)
            _emit_event(
                event_fn,
                IngestTeamFailedEvent(
                    team_index=team_index,
                    team_total=team_total,
                    failure=failure,
                ),
            )

    return SeasonRangeResult(
        seasons=seasons,
        attempted_team_seasons=attempted_team_seasons,
        completed_team_seasons=completed_team_seasons,
        failures=failures,
    )


def _build_seasons(
    *,
    start_year: int,
    end_year: int,
    season_type: str,
    season_str: str | None,
) -> list[Season]:
    if season_str is not None:
        return [Season(season_str, season_type)]
    return build_season_list(
        start_year,
        end_year,
        season_type,
    )


def _resolve_teams(*, team_abbreviations: list[str] | None, season: Season) -> list[Team]:
    if team_abbreviations is None:
        return Team.all_active_in_season(season)
    return [
        Team.from_abbreviation(team_abbreviation, season=season)
        for team_abbreviation in team_abbreviations
    ]


def _build_progress_fn(
    event_fn: IngestEventFn | None,
    team_index: int,
    team_total: int,
) -> _TeamProgressFn | None:
    if event_fn is None:
        return None

    def _progress(progress: IngestProgress) -> None:
        _emit_event(
            event_fn,
            IngestTeamProgressEvent(
                team_index=team_index,
                team_total=team_total,
                progress=progress,
            ),
        )

    return _progress


def _build_source_update_fn(
    *,
    request: IngestRequest,
    progress_fn: _TeamProgressFn | None,
) -> Callable[[NbaApiGameIngestUpdate], None] | None:
    if progress_fn is None:
        return None

    def _source_update(update: NbaApiGameIngestUpdate) -> None:
        _emit_progress(
            progress_fn,
            request=request,
            current=update.current,
            total=update.total,
            status=update.status,
            game_id=update.game_id,
        )

    return _source_update


def _emit_progress(
    progress: _TeamProgressFn | None,
    *,
    request: IngestRequest,
    current: int,
    total: int,
    status: str,
    game_id: str | None = None,
) -> None:
    if progress is None:
        return
    progress(
        IngestProgress(
            team=request.team,
            season=request.season,
            current=current,
            total=total,
            status=status,
            game_id=game_id,
        )
    )


def _emit_event(event_fn: IngestEventFn | None, event: IngestEvent) -> None:
    if event_fn is None:
        return
    event_fn(event)


def _append_failure_log(
    failure_log_fn: FailureLogFn | None,
    failure: SeasonRangeFailure,
) -> None:
    if failure_log_fn is None:
        return
    failure_log_fn(failure.to_log_entry())


__all__ = [
    "FailureLogFn",
    "IngestEvent",
    "IngestEventFn",
    "IngestFailureLogEntry",
    "IngestProgress",
    "IngestRequest",
    "IngestResult",
    "IngestSeasonStartedEvent",
    "IngestSummary",
    "IngestTeamCompletedEvent",
    "IngestTeamFailedEvent",
    "IngestTeamProgressEvent",
    "SeasonRangeFailure",
    "SeasonRangeResult",
    "refresh_season_range",
    "refresh_team_season",
]
