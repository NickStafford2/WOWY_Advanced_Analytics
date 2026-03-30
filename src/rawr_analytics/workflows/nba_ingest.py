from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from rawr_analytics.cli import filtered_log
from rawr_analytics.data.game_cache.repository import replace_team_season_normalized_rows
from rawr_analytics.nba.errors import FetchError, GameNormalizationFailure, PartialTeamSeasonError
from rawr_analytics.nba.models import (
    NormalizedGamePlayerRecord,
    NormalizedGameRecord,
    NormalizedTeamSeasonBatch,
)
from rawr_analytics.nba.normalize.normalize_game import normalize_source_league_game
from rawr_analytics.nba.normalize.validation import validate_normalized_team_season_batch
from rawr_analytics.nba.source.api import load_or_fetch_box_score
from rawr_analytics.nba.source.cache import load_or_fetch_league_games
from rawr_analytics.nba.source.dedupe import dedupe_schedule_games
from rawr_analytics.nba.source.models import SourceLeagueGame
from rawr_analytics.nba.source.parsers import parse_league_schedule_payload
from rawr_analytics.shared.common import LogFn, ProgressFn
from rawr_analytics.shared.season import Season, build_season_list
from rawr_analytics.shared.team import Team

_TeamSeasonFailureError = FetchError | PartialTeamSeasonError | ValueError
_SeasonStartedFn = Callable[[int, int, Season], None]
_TeamFinishedFn = Callable[[int, int, "IngestResult"], None]
_TeamFailedFn = Callable[[int, int, "SeasonRangeFailure"], None]


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
    log_fn: LogFn | None = filtered_log,
    progress_fn: Callable[[int, int, dict], None] | None = None,
    season_started_fn: _SeasonStartedFn | None = None,
    team_completed_fn: _TeamFinishedFn | None = None,
    team_failed_fn: _TeamFailedFn | None = None,
) -> SeasonRangeResult:
    seasons = _build_seasons(
        season=season_str,
        start_year=start_year,
        end_year=end_year,
        season_type=season_type,
    )
    season_total = len(seasons)
    attempted_team_seasons = 0
    completed_team_seasons = 0
    failures: list[SeasonRangeFailure] = []

    for season_index, current_season in enumerate(seasons, start=1):
        if season_started_fn is not None:
            season_started_fn(season_index, season_total, current_season)

        teams = _resolve_teams(team_codes=team_abbreviations, season=current_season)
        team_total = len(teams)
        for team_index, team in enumerate(teams, start=1):
            attempted_team_seasons += 1
            request = IngestRequest(team, current_season)
            try:
                result = refresh_team_season(
                    request,
                    log=log_fn,
                    progress=(
                        None
                        if progress_fn is None
                        else lambda payload,
                        team_index=team_index,
                        team_total=team_total: progress_fn(
                            team_index,
                            team_total,
                            payload,
                        )
                    ),
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


def _get_schedule(
    request: IngestRequest,
    log_fn: LogFn | None = print,
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


def _ingest(
    request: IngestRequest,
    *,
    log_fn: LogFn | None = print,
    progress_fn: ProgressFn | None = None,
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
                game_id=schedule_game.game_id, log_fn=log_fn
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
                reason: examples[:] for reason, examples in sorted(failure_reason_examples.items())
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


def _store(result: IngestResult) -> None:
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


def _build_seasons(
    *,
    season: str | None,
    start_year: int,
    end_year: int,
    season_type: str,
) -> list[Season]:
    if season is not None:
        return [Season(season, season_type)]
    return build_season_list(start_year, end_year, season_type)


def _resolve_teams(*, team_codes: list[str] | None, season: Season) -> list[Team]:
    if team_codes is None:
        return Team.all_active_in_season(season)
    return [Team.from_abbreviation(team_code, season=season) for team_code in team_codes]


def refresh_team_season(
    request: IngestRequest,
    *,
    log: LogFn | None = print,
    progress: ProgressFn | None = None,
) -> IngestResult:
    result = _ingest(
        request,
        log_fn=log,
        progress_fn=progress,
    )
    _store(result)
    return result


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
    progress: ProgressFn | None,
    *,
    request: IngestRequest,
    current: int,
    total: int,
    status: str,
    game_id: str | None = None,
) -> None:
    if progress is None:
        return
    payload = {
        "team": request.team,
        "season": request.season.id,
        "current": current,
        "total": total,
        "status": status,
    }
    if game_id is not None:
        payload["game_id"] = game_id
    progress(payload)


__all__ = [
    "IngestRequest",
    "IngestResult",
    "IngestSummary",
    "SeasonRangeFailure",
    "SeasonRangeResult",
    "refresh_season_range",
    "refresh_team_season",
]
