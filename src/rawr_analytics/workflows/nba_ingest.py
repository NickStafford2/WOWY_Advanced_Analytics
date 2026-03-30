from __future__ import annotations

from dataclasses import dataclass

from rawr_analytics.data.game_cache.repository import replace_team_season_normalized_rows
from rawr_analytics.nba.errors import GameNormalizationFailure, PartialTeamSeasonError
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
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


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
    "refresh_team_season",
    "_ingest",
    "_store",
]
