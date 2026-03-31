from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from rawr_analytics.nba.errors import (
    BoxScoreFetchError,
    FetchError,
    GameNormalizationFailure,
    LeagueGamesFetchError,
    PartialTeamSeasonError,
)
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]

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
        team=_build_logged_team(team),
        season=_build_logged_season(season),
        failure_kind=failure_kind,
        error_type=type(error).__name__,
        message=str(error),
        details=_build_error_details(error),
    )

    log_path: Path = _DEFAULT_INGEST_FAILURE_LOG_PATH
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(_serialize_record(record), sort_keys=True))
        log_file.write("\n")


def _build_error_details(
    error: Exception,
) -> _FetchErrorLogDetails | _PartialTeamSeasonLogDetails | None:
    if isinstance(error, PartialTeamSeasonError):
        return _build_partial_team_season_details(error)
    if isinstance(error, FetchError):
        return _build_fetch_error_details(error)
    return None


def _build_fetch_error_details(error: FetchError) -> _FetchErrorLogDetails:
    if isinstance(error, LeagueGamesFetchError):
        return _FetchErrorLogDetails(
            resource=error.resource,
            identifier=error.identifier,
            attempts=error.attempts,
            last_error_type=error.last_error_type,
            last_error_message=error.last_error_message,
            team_scope=_build_logged_team(error.team),
            season_scope=_build_logged_season(error.season),
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


def _build_partial_team_season_details(
    error: PartialTeamSeasonError,
) -> _PartialTeamSeasonLogDetails:
    return _PartialTeamSeasonLogDetails(
        team_scope=_build_logged_team(error.team),
        season_scope=_build_logged_season(error.season),
        failed_game_ids=list(error.failed_game_ids),
        failed_game_details=[
            _build_logged_game_failure(failure) for failure in error.failed_game_details
        ],
        failure_reason_counts=dict(error.failure_reason_counts),
        failure_reason_examples={
            reason: list(example_game_ids)
            for reason, example_game_ids in error.failure_reason_examples.items()
        },
        total_games=error.total_games,
        failed_games=error.failed_games,
    )


def _build_logged_team(team: Team) -> _LoggedTeam:
    return _LoggedTeam(
        team_id=team.team_id,
        abbreviation=team.current.abbreviation,
    )


def _build_logged_season(season: Season) -> _LoggedSeason:
    return _LoggedSeason(
        id=season.id,
        start_year=season.start_year,
        season_type=season.season_type.value,
    )


def _build_logged_game_failure(failure: GameNormalizationFailure) -> _LoggedGameFailure:
    return _LoggedGameFailure(
        game_id=failure.game_id,
        error_type=failure.error_type,
        message=failure.message,
    )


def _serialize_record(record: _IngestFailureLogRecord) -> JsonObject:
    payload: JsonObject = {
        "timestamp_utc": record.timestamp_utc,
        "team": _serialize_logged_team(record.team),
        "season": _serialize_logged_season(record.season),
        "failure_kind": record.failure_kind,
        "error_type": record.error_type,
        "message": record.message,
    }
    if record.details is not None:
        payload["details"] = _serialize_error_details(record.details)
    return payload


def _serialize_error_details(
    details: _FetchErrorLogDetails | _PartialTeamSeasonLogDetails,
) -> JsonObject:
    if isinstance(details, _FetchErrorLogDetails):
        return _serialize_fetch_error_details(details)
    return _serialize_partial_team_season_details(details)


def _serialize_fetch_error_details(details: _FetchErrorLogDetails) -> JsonObject:
    payload: JsonObject = {
        "resource": details.resource,
        "identifier": details.identifier,
        "attempts": details.attempts,
        "last_error_type": details.last_error_type,
        "last_error_message": details.last_error_message,
    }
    if details.team_scope is not None:
        payload["team_scope"] = _serialize_logged_team(details.team_scope)
    if details.season_scope is not None:
        payload["season_scope"] = _serialize_logged_season(details.season_scope)
    if details.game_id is not None:
        payload["game_id"] = details.game_id
    return payload


def _serialize_partial_team_season_details(details: _PartialTeamSeasonLogDetails) -> JsonObject:
    return {
        "team_scope": _serialize_logged_team(details.team_scope),
        "season_scope": _serialize_logged_season(details.season_scope),
        "failed_game_ids": list(details.failed_game_ids),
        "failed_game_details": [
            _serialize_logged_game_failure(failure) for failure in details.failed_game_details
        ],
        "failure_reason_counts": {
            reason: count for reason, count in details.failure_reason_counts.items()
        },
        "failure_reason_examples": {
            reason: list(example_game_ids)
            for reason, example_game_ids in details.failure_reason_examples.items()
        },
        "total_games": details.total_games,
        "failed_games": details.failed_games,
    }


def _serialize_logged_team(team: _LoggedTeam) -> JsonObject:
    return {
        "team_id": team.team_id,
        "abbreviation": team.abbreviation,
    }


def _serialize_logged_season(season: _LoggedSeason) -> JsonObject:
    return {
        "id": season.id,
        "start_year": season.start_year,
        "season_type": season.season_type,
    }


def _serialize_logged_game_failure(failure: _LoggedGameFailure) -> JsonObject:
    return {
        "game_id": failure.game_id,
        "error_type": failure.error_type,
        "message": failure.message,
    }
