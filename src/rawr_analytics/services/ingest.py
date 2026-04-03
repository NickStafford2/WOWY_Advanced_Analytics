from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.data.game_cache import replace_team_season_normalized_rows
from rawr_analytics.nba import FetchError, PartialTeamSeasonError
from rawr_analytics.nba.errors import GameNormalizationFailure
from rawr_analytics.nba.normalize import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    NormalizedTeamSeasonBatch,
    normalize_source_league_game,
    validate_normalized_team_season_batch,
)
from rawr_analytics.nba.source import (
    SourceLeagueGame,
    dedupe_schedule_games,
    load_or_fetch_box_score,
    load_or_fetch_league_games,
    parse_league_schedule_payload,
)
from rawr_analytics.shared.common import LogFn
from rawr_analytics.shared.season import Season, build_season_list
from rawr_analytics.shared.team import Team

_TeamProgressFn = Callable[["IngestProgress"], None]
IngestEventFn = Callable[["IngestEvent"], None]
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
            team=self.request.team,
            season=self.request.season,
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
class IngestRefreshRequest:
    start_year: int
    end_year: int
    season_type: str
    season_str: str | None = None
    team_abbreviations: list[str] | None = None


def refresh_team_season(
    request: IngestRequest,
    *,
    log: LogFn | None = print,
    progress: _TeamProgressFn | None = None,
) -> IngestResult:
    result = _ingest_team_season(
        request,
        log_fn=log,
        progress_fn=progress,
    )
    _store_team_season(result)
    return result


def refresh_season_range(
    request: IngestRefreshRequest,
    *,
    log_fn: LogFn | None = None,
    event_fn: IngestEventFn | None = None,
) -> SeasonRangeResult:
    seasons = _build_seasons(request=request)
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

        teams = _resolve_teams(team_abbreviations=request.team_abbreviations, season=season)
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


def _build_seasons(*, request: IngestRefreshRequest) -> list[Season]:
    if request.season_str is not None:
        return [Season(request.season_str, request.season_type)]
    return build_season_list(
        request.start_year,
        request.end_year,
        request.season_type,
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


def _get_schedule(
    request: IngestRequest,
    log_fn: LogFn | None,
) -> tuple[list[SourceLeagueGame], str]:
    schedule_payload, league_games_source = load_or_fetch_league_games(
        team=request.team,
        season=request.season,
        log_fn=log_fn,
    )
    schedule = parse_league_schedule_payload(
        schedule_payload,
        team=request.team,
        season=request.season,
    )
    games = dedupe_schedule_games(schedule.games)
    return games, league_games_source


def _ingest_team_season(
    request: IngestRequest,
    *,
    log_fn: LogFn | None,
    progress_fn: _TeamProgressFn | None,
) -> IngestResult:
    schedule_games, league_games_source = _get_schedule(request, log_fn)
    total_games = len(schedule_games)

    games: list[NormalizedGameRecord] = []
    game_players: list[NormalizedGamePlayerRecord] = []
    failures: list[GameNormalizationFailure] = []
    failure_reason_counts: dict[str, int] = {}
    failure_reason_examples: dict[str, list[str]] = {}
    fetched_box_scores = 0
    cached_box_scores = 0

    _emit_progress(
        progress_fn,
        request=request,
        current=0,
        total=total_games,
        status="schedule-loaded",
    )

    for index, schedule_game in enumerate(schedule_games, start=1):
        try:
            box_score, box_score_source = load_or_fetch_box_score(
                game_id=schedule_game.game_id,
                log_fn=log_fn,
            )
            game, players = normalize_source_league_game(
                source_league_game=schedule_game,
                box_score=box_score,
                season=request.season,
            )
        except ValueError as exc:
            _record_failure(
                failures=failures,
                failure_reason_counts=failure_reason_counts,
                failure_reason_examples=failure_reason_examples,
                game_id=schedule_game.game_id,
                exc=exc,
            )
            if log_fn is not None:
                log_fn(f"failed {request.label} game={schedule_game.game_id} reason={exc}")
            _emit_progress(
                progress_fn,
                request=request,
                current=index,
                total=total_games,
                status="failed",
                game_id=schedule_game.game_id,
            )
            continue

        if box_score_source == "fetched":
            fetched_box_scores += 1
        else:
            cached_box_scores += 1
        games.append(game)
        game_players.extend(players)
        _emit_progress(
            progress_fn,
            request=request,
            current=index,
            total=total_games,
            status="ok",
            game_id=schedule_game.game_id,
        )

    if failures:
        raise PartialTeamSeasonError(
            message=(
                f"Incomplete team-season ingest for {request.label}: "
                f"{len(failures)}/{total_games} games failed normalization"
            ),
            team=request.team,
            season=request.season,
            failed_game_ids=[failure.game_id for failure in failures],
            total_games=total_games,
            failed_games=len(failures),
            failed_game_details=failures,
            failure_reason_counts=dict(sorted(failure_reason_counts.items())),
            failure_reason_examples={
                reason: examples[:]
                for reason, examples in sorted(failure_reason_examples.items())
            },
        )

    result = IngestResult(
        request=request,
        games=games,
        game_players=game_players,
        summary=IngestSummary(
            total_games=total_games,
            processed_games=len(games),
            fetched_box_scores=fetched_box_scores,
            cached_box_scores=cached_box_scores,
            league_games_source=league_games_source,
        ),
    )
    validate_normalized_team_season_batch(result.to_batch())
    return result


def _store_team_season(result: IngestResult) -> None:
    replace_team_season_normalized_rows(
        team=result.request.team,
        season=result.request.season,
        games=result.games,
        game_players=result.game_players,
        source_path=(
            "sqlite://normalized_games/"
            f"{result.request.team.abbreviation(season=result.request.season)}_"
            f"{result.request.season.id}_{result.request.season.season_type.to_slug()}"
        ),
        source_snapshot="ingest-build-v2",
        source_kind="nba-api",
        expected_games_row_count=result.summary.total_games,
        skipped_games_row_count=0,
    )


def _record_failure(
    *,
    failures: list[GameNormalizationFailure],
    failure_reason_counts: dict[str, int],
    failure_reason_examples: dict[str, list[str]],
    game_id: str,
    exc: Exception,
) -> None:
    failure = GameNormalizationFailure(
        game_id=game_id,
        error_type=type(exc).__name__,
        message=str(exc),
    )
    failures.append(failure)
    failure_reason_counts[failure.message] = failure_reason_counts.get(failure.message, 0) + 1
    failure_reason_examples.setdefault(failure.message, [])
    if len(failure_reason_examples[failure.message]) < 3:
        failure_reason_examples[failure.message].append(game_id)


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


__all__ = [
    "IngestEvent",
    "IngestEventFn",
    "IngestProgress",
    "IngestRefreshRequest",
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
