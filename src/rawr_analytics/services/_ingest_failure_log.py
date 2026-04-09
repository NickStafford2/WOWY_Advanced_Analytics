from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from rawr_analytics.shared.ingest import (
    BoxScoreFetchError,
    FetchError,
    GameNormalizationFailure,
    LeagueGamesFetchError,
    PartialTeamSeasonError,
)
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

_DEFAULT_INGEST_FAILURE_LOG_PATH = Path("data/logs/ingest_failures.jsonl")


@dataclass(frozen=True)
class _LoggedSeason:
    id: str
    start_year: int
    season_type: str


@dataclass(frozen=True)
class _LoggedTeam:
    team_id: int
    abbreviation: str


@dataclass(frozen=True)
class _LoggedGameFailure:
    game_id: str
    error_type: str
    message: str


@dataclass(frozen=True)
class _FetchErrorLogDetails:
    resource: str
    identifier: str
    attempts: int
    last_error_type: str
    last_error_message: str
    team_scope: _LoggedTeam | None = None
    season_scope: _LoggedSeason | None = None
    game_id: str | None = None


@dataclass(frozen=True)
class _PartialTeamSeasonLogDetails:
    team_scope: _LoggedTeam
    season_scope: _LoggedSeason
    failed_game_ids: list[str]
    failed_game_details: list[_LoggedGameFailure]
    failure_reason_counts: dict[str, int]
    failure_reason_examples: dict[str, list[str]]
    total_games: int
    failed_games: int


@dataclass(frozen=True)
class _IngestFailureLogRecord:
    timestamp_utc: str
    team: _LoggedTeam
    season: _LoggedSeason
    failure_kind: str
    error_type: str
    message: str
    details: _FetchErrorLogDetails | _PartialTeamSeasonLogDetails | None


def append_ingest_failure_log(
    *,
    team: Team,
    season: Season,
    failure_kind: str,
    error: Exception,
) -> None:
    record = _IngestFailureLogRecord(
        timestamp_utc=datetime.now(UTC).isoformat(),
        team=_logged_team(team),
        season=_logged_season(season),
        failure_kind=failure_kind,
        error_type=type(error).__name__,
        message=str(error),
        details=_error_details(error),
    )

    log_path = _DEFAULT_INGEST_FAILURE_LOG_PATH
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(asdict(record), sort_keys=True))
        log_file.write("\n")


def _error_details(
    error: Exception,
) -> _FetchErrorLogDetails | _PartialTeamSeasonLogDetails | None:
    if isinstance(error, PartialTeamSeasonError):
        return _partial_team_season_details(error)
    if isinstance(error, FetchError):
        return _fetch_error_details(error)
    return None


def _fetch_error_details(error: FetchError) -> _FetchErrorLogDetails:
    if isinstance(error, LeagueGamesFetchError):
        return _FetchErrorLogDetails(
            resource=error.resource,
            identifier=error.identifier,
            attempts=error.attempts,
            last_error_type=error.last_error_type,
            last_error_message=error.last_error_message,
            team_scope=_logged_team(error.team),
            season_scope=_logged_season(error.season),
        )
    if isinstance(error, BoxScoreFetchError):
        return _FetchErrorLogDetails(
            resource=error.resource,
            identifier=error.identifier,
            attempts=error.attempts,
            last_error_type=error.last_error_type,
            last_error_message=error.last_error_message,
            game_id=error.game_id,
        )
    return _FetchErrorLogDetails(
        resource=error.resource,
        identifier=error.identifier,
        attempts=error.attempts,
        last_error_type=error.last_error_type,
        last_error_message=error.last_error_message,
    )


def _partial_team_season_details(
    error: PartialTeamSeasonError,
) -> _PartialTeamSeasonLogDetails:
    return _PartialTeamSeasonLogDetails(
        team_scope=_logged_team(error.team),
        season_scope=_logged_season(error.season),
        failed_game_ids=list(error.failed_game_ids),
        failed_game_details=[_logged_game_failure(f) for f in error.failed_game_details],
        failure_reason_counts={**error.failure_reason_counts},
        failure_reason_examples={
            reason: list(example_ids)
            for reason, example_ids in error.failure_reason_examples.items()
        },
        total_games=error.total_games,
        failed_games=error.failed_games,
    )


def _logged_team(team: Team) -> _LoggedTeam:
    return _LoggedTeam(
        team_id=team.team_id,
        abbreviation=team.current.abbreviation,
    )


def _logged_season(season: Season) -> _LoggedSeason:
    return _LoggedSeason(
        id=season.year_string_nba_api,
        start_year=season.start_year,
        season_type=season.season_type.value,
    )


def _logged_game_failure(failure: GameNormalizationFailure) -> _LoggedGameFailure:
    return _LoggedGameFailure(
        game_id=failure.game_id,
        error_type=failure.error_type,
        message=failure.message,
    )
