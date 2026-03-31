from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.nba.errors import FetchError, PartialTeamSeasonError
from rawr_analytics.shared.common import LogFn
from rawr_analytics.shared.season import Season, build_season_list
from rawr_analytics.shared.team import Team
from rawr_analytics.workflows.nba_ingest import (
    IngestProgress,
    IngestRequest,
    IngestResult,
    refresh_team_season,
)

_TeamSeasonFailureError = FetchError | PartialTeamSeasonError | ValueError
_SeasonStartedFn = Callable[[int, int, Season], None]
_TeamCompletedFn = Callable[[int, int, IngestResult], None]
_TeamFailedFn = Callable[[int, int, "SeasonRangeFailure"], None]
_ProgressFn = Callable[[int, int, IngestProgress], None]


@dataclass(frozen=True)
class SeasonRangeFailure:
    request: IngestRequest
    failure_kind: str
    error: _TeamSeasonFailureError

    @property
    def scope(self) -> str:
        return self.request.label


@dataclass(frozen=True)
class SeasonRangeResult:
    seasons: list[Season]
    attempted_team_seasons: int
    completed_team_seasons: int
    failures: list[SeasonRangeFailure]

    @property
    def failure_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for failure in self.failures:
            counts[failure.failure_kind] = counts.get(failure.failure_kind, 0) + 1
        return counts

    @property
    def failed_scopes(self) -> list[str]:
        return [failure.scope for failure in self.failures]

    @property
    def exit_status(self) -> int:
        return 1 if self.failures else 0


def refresh_season_range(
    *,
    season_str: str | None = None,
    start_year: int,
    end_year: int,
    season_type: str,
    team_abbreviations: list[str] | None = None,
    log_fn: LogFn | None = None,
    progress_fn: _ProgressFn | None = None,
    season_started_fn: _SeasonStartedFn | None = None,
    team_completed_fn: _TeamCompletedFn | None = None,
    team_failed_fn: _TeamFailedFn | None = None,
) -> SeasonRangeResult:
    seasons = _build_seasons(
        season_str=season_str,
        start_year=start_year,
        end_year=end_year,
        season_type=season_type,
    )
    season_total = len(seasons)
    attempted_team_seasons = 0
    completed_team_seasons = 0
    failures: list[SeasonRangeFailure] = []

    for season_index, season in enumerate(seasons, start=1):
        if season_started_fn is not None:
            season_started_fn(season_index, season_total, season)

        teams = _resolve_teams(team_abbreviations=team_abbreviations, season=season)
        team_total = len(teams)
        for team_index, team in enumerate(teams, start=1):
            attempted_team_seasons += 1
            request = IngestRequest(team, season)
            try:
                result = refresh_team_season(
                    request,
                    log=log_fn,
                    progress=_build_progress_fn(progress_fn, team_index, team_total),
                )
            except FetchError as exc:
                failure = SeasonRangeFailure(
                    request=request,
                    failure_kind="fetch_error",
                    error=exc,
                )
            except PartialTeamSeasonError as exc:
                failure = SeasonRangeFailure(
                    request=request,
                    failure_kind="partial_scope_error",
                    error=exc,
                )
            except ValueError as exc:
                failure = SeasonRangeFailure(
                    request=request,
                    failure_kind="validation_error",
                    error=exc,
                )
            else:
                completed_team_seasons += 1
                if team_completed_fn is not None:
                    team_completed_fn(team_index, team_total, result)
                continue

            failures.append(failure)
            if team_failed_fn is not None:
                team_failed_fn(team_index, team_total, failure)

    return SeasonRangeResult(
        seasons=seasons,
        attempted_team_seasons=attempted_team_seasons,
        completed_team_seasons=completed_team_seasons,
        failures=failures,
    )


def _build_seasons(
    *,
    season_str: str | None,
    start_year: int,
    end_year: int,
    season_type: str,
) -> list[Season]:
    if season_str is not None:
        return [Season(season_str, season_type)]
    return build_season_list(start_year, end_year, season_type)


def _resolve_teams(*, team_abbreviations: list[str] | None, season: Season) -> list[Team]:
    if team_abbreviations is None:
        return Team.all_active_in_season(season)
    return [
        Team.from_abbreviation(team_abbreviation, season=season)
        for team_abbreviation in team_abbreviations
    ]


def _build_progress_fn(
    progress_fn: _ProgressFn | None,
    team_index: int,
    team_total: int,
) -> Callable[[IngestProgress], None] | None:
    if progress_fn is None:
        return None

    def _progress(progress: IngestProgress) -> None:
        progress_fn(team_index, team_total, progress)

    return _progress


__all__ = [
    "SeasonRangeFailure",
    "SeasonRangeResult",
    "refresh_season_range",
]
