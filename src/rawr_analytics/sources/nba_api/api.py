from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rawr_analytics.services._ingest_errors import GameNormalizationFailure, PartialTeamSeasonError
from rawr_analytics.sources.nba_api.cache import load_or_fetch_box_score_cache
from rawr_analytics.sources.nba_api.cache import load_or_fetch_league_games
from rawr_analytics.sources.nba_api.dedupe import dedupe_schedule_games
from rawr_analytics.sources.nba_api.models import SourceLeagueGame
from rawr_analytics.sources.nba_api.models import SourceBoxScore
from rawr_analytics.sources.nba_api.normalize import normalize_source_league_game
from rawr_analytics.sources.nba_api.parsers import parse_box_score_payload
from rawr_analytics.sources.nba_api.parsers import parse_league_schedule_payload
from rawr_analytics.shared.game import NormalizedGamePlayerRecord, NormalizedGameRecord
from rawr_analytics.shared.common import LogFn
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team


@dataclass(frozen=True)
class NbaApiGameIngestUpdate:
    current: int
    total: int
    status: str
    game_id: str | None = None


@dataclass(frozen=True)
class NbaApiTeamSeasonData:
    games: list[NormalizedGameRecord]
    game_players: list[NormalizedGamePlayerRecord]
    total_games: int
    fetched_box_scores: int
    cached_box_scores: int
    league_games_source: str


GameIngestUpdateFn = Callable[[NbaApiGameIngestUpdate], None]


def load_or_fetch_box_score(
    game_id: str,
    log_fn: LogFn | None = print,
) -> tuple[SourceBoxScore, str]:
    box_score_payload, box_score_source = load_or_fetch_box_score_cache(game_id, log_fn)

    box_score = parse_box_score_payload(
        box_score_payload,
        game_id=game_id,
    )
    return box_score, box_score_source


def ingest_team_season(
    *,
    team: Team,
    season: Season,
    log_fn: LogFn | None = print,
    update_fn: GameIngestUpdateFn | None = None,
) -> NbaApiTeamSeasonData:
    schedule_games, league_games_source = _load_schedule_games(
        team=team,
        season=season,
        log_fn=log_fn,
    )
    total_games = len(schedule_games)
    _emit_update(update_fn, current=0, total=total_games, status="schedule-loaded")

    games: list[NormalizedGameRecord] = []
    game_players: list[NormalizedGamePlayerRecord] = []
    failures: list[GameNormalizationFailure] = []
    failure_reason_counts: dict[str, int] = {}
    failure_reason_examples: dict[str, list[str]] = {}
    fetched_box_scores = 0
    cached_box_scores = 0

    for index, schedule_game in enumerate(schedule_games, start=1):
        try:
            box_score, box_score_source = load_or_fetch_box_score(
                game_id=schedule_game.game_id,
                log_fn=log_fn,
            )
            game, players = normalize_source_league_game(
                source_league_game=schedule_game,
                box_score=box_score,
                season=season,
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
                log_fn(
                    "failed "
                    f"{team.abbreviation(season=season)} {season} "
                    f"game={schedule_game.game_id} reason={exc}"
                )
            _emit_update(
                update_fn,
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
        _emit_update(
            update_fn,
            current=index,
            total=total_games,
            status="ok",
            game_id=schedule_game.game_id,
        )

    if failures:
        raise PartialTeamSeasonError(
            message=(
                f"Incomplete team-season ingest for {team.abbreviation(season=season)} {season}: "
                f"{len(failures)}/{total_games} games failed normalization"
            ),
            team=team,
            season=season,
            failed_game_ids=[failure.game_id for failure in failures],
            total_games=total_games,
            failed_games=len(failures),
            failed_game_details=failures,
            failure_reason_counts=dict(sorted(failure_reason_counts.items())),
            failure_reason_examples={
                reason: examples[:] for reason, examples in sorted(failure_reason_examples.items())
            },
        )

    return NbaApiTeamSeasonData(
        games=games,
        game_players=game_players,
        total_games=total_games,
        fetched_box_scores=fetched_box_scores,
        cached_box_scores=cached_box_scores,
        league_games_source=league_games_source,
    )


def _load_schedule_games(
    *,
    team: Team,
    season: Season,
    log_fn: LogFn | None,
) -> tuple[list[SourceLeagueGame], str]:
    schedule_payload, league_games_source = load_or_fetch_league_games(
        team=team,
        season=season,
        log_fn=log_fn,
    )
    schedule = parse_league_schedule_payload(
        schedule_payload,
        team=team,
        season=season,
    )
    return dedupe_schedule_games(schedule.games), league_games_source


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


def _emit_update(
    update_fn: GameIngestUpdateFn | None,
    *,
    current: int,
    total: int,
    status: str,
    game_id: str | None = None,
) -> None:
    if update_fn is None:
        return
    update_fn(
        NbaApiGameIngestUpdate(
            current=current,
            total=total,
            status=status,
            game_id=game_id,
        )
    )


__all__ = [
    "GameIngestUpdateFn",
    "NbaApiGameIngestUpdate",
    "NbaApiTeamSeasonData",
    "ingest_team_season",
    "load_or_fetch_box_score",
]
