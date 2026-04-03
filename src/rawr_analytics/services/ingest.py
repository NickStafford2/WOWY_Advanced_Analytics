from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.game_cache import replace_team_season_normalized_rows
from rawr_analytics.data.game_cache.rows import NormalizedGamePlayerRow, NormalizedGameRow
from rawr_analytics.services._ingest_validation import validate_normalized_team_season_batch
from rawr_analytics.services._ingest_errors import (
    FetchError,
    PartialTeamSeasonError,
)
from rawr_analytics.shared.game import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    NormalizedTeamSeasonBatch,
)
from rawr_analytics.sources.nba_api import (
    NbaApiGameIngestUpdate,
    ingest_team_season,
)
from rawr_analytics.shared.common import LogFn
from rawr_analytics.shared.scope import TeamSeasonScope
from rawr_analytics.shared.season import Season, build_season_list
from rawr_analytics.shared.team import Team

_TeamProgressFn = Callable[["IngestProgress"], None]
IngestEventFn = Callable[["IngestEvent"], None]
FailureLogFn = Callable[["IngestFailureLogEntry"], None]
_TeamSeasonFailureError = FetchError | PartialTeamSeasonError | ValueError


@dataclass(frozen=True)
class IngestRequest:
    team: Team
    season: Season

    @property
    def label(self) -> str:
        return f"{self.team.abbreviation(season=self.season)} {self.season}"


@dataclass(frozen=True)
class IngestSummary:
    total_games: int
    processed_games: int
    fetched_box_scores: int
    cached_box_scores: int
    league_games_source: str


@dataclass(frozen=True)
class IngestProgress:
    team: Team
    season: Season
    current: int
    total: int
    status: str
    game_id: str | None = None


@dataclass(frozen=True)
class IngestSeasonStartedEvent:
    season_index: int
    season_total: int
    season: Season


@dataclass(frozen=True)
class IngestTeamProgressEvent:
    team_index: int
    team_total: int
    progress: IngestProgress


@dataclass(frozen=True)
class IngestResult:
    request: IngestRequest
    games: list[NormalizedGameRecord]
    game_players: list[NormalizedGamePlayerRecord]
    summary: IngestSummary

    def to_batch(self) -> NormalizedTeamSeasonBatch:
        return NormalizedTeamSeasonBatch(
            scope=TeamSeasonScope(team=self.request.team, season=self.request.season),
            games=self.games,
            game_players=self.game_players,
        )


@dataclass(frozen=True)
class IngestTeamCompletedEvent:
    team_index: int
    team_total: int
    result: IngestResult


@dataclass(frozen=True)
class SeasonRangeFailure:
    request: IngestRequest
    failure_kind: str
    error: _TeamSeasonFailureError

    @property
    def scope(self) -> str:
        return self.request.label

    def to_log_entry(self) -> IngestFailureLogEntry:
        return IngestFailureLogEntry(
            scope=self.scope,
            team=self.request.team,
            season=self.request.season,
            failure_kind=self.failure_kind,
            error=self.error,
        )


@dataclass(frozen=True)
class IngestTeamFailedEvent:
    team_index: int
    team_total: int
    failure: SeasonRangeFailure


IngestEvent = (
    IngestSeasonStartedEvent
    | IngestTeamProgressEvent
    | IngestTeamCompletedEvent
    | IngestTeamFailedEvent
)


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


@dataclass(frozen=True)
class IngestFailureLogEntry:
    scope: str
    team: Team
    season: Season
    failure_kind: str
    error: _TeamSeasonFailureError


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
    _store_team_season(result)
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


def _store_team_season(result: IngestResult) -> None:
    replace_team_season_normalized_rows(
        team=result.request.team,
        season=result.request.season,
        games=[_build_game_cache_game_row(game) for game in result.games],
        game_players=[_build_game_cache_game_player_row(player) for player in result.game_players],
        source_path=(
            "sqlite://normalized_games/"
            f"{result.request.team.abbreviation(season=result.request.season)}_"
            f"{result.request.season.id}_{result.request.season.season_type.to_slug()}"
        ),
        source_snapshot="ingest-build-v2",
        source_kind="nba_api",
        expected_games_row_count=result.summary.total_games,
        skipped_games_row_count=0,
    )


def _build_game_cache_game_row(game: NormalizedGameRecord) -> NormalizedGameRow:
    return NormalizedGameRow(
        game_id=game.game_id,
        game_date=game.game_date,
        season=game.season,
        team=game.team,
        opponent_team=game.opponent_team,
        is_home=game.is_home,
        margin=game.margin,
        source=game.source,
    )


def _build_game_cache_game_player_row(
    player: NormalizedGamePlayerRecord,
) -> NormalizedGamePlayerRow:
    return NormalizedGamePlayerRow(
        game_id=player.game_id,
        player=player.player,
        appeared=player.appeared,
        minutes=player.minutes,
        team=player.team,
    )


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
