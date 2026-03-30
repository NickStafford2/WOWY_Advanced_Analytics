from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from rawr_analytics.nba.errors import (
    FetchError,
    PartialTeamSeasonError,
)
from rawr_analytics.shared.season import Season
from rawr_analytics.shared.team import Team

_DEFAULT_INGEST_FAILURE_LOG_PATH = Path("data/logs/ingest_failures.jsonl")


def append_ingest_failure_log(
    *,
    team: Team,
    season: Season,
    failure_kind: str,
    error: Exception,
) -> None:
    record: dict[str, Any] = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "team": team,
        "season": season,
        "failure_kind": failure_kind,
        "error_type": type(error).__name__,
        "message": str(error),
    }
    record.update(_build_error_details(error))

    log_path: Path = _DEFAULT_INGEST_FAILURE_LOG_PATH
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(_to_json_value(record), sort_keys=True))
        log_file.write("\n")


def _build_error_details(error: Exception) -> dict[str, object]:
    if isinstance(error, FetchError):
        details = {
            "resource": error.resource,
            "identifier": error.identifier,
            "attempts": error.attempts,
            "last_error_type": error.last_error_type,
            "last_error_message": error.last_error_message,
        }
        details.update(_dataclass_fields(error, exclude={"message"}))
        return details
    if isinstance(error, PartialTeamSeasonError):
        return {
            "team_scope": error.team,
            "season_scope": error.season,
            "season_type_scope": error.season.season_type,
            "failed_game_ids": error.failed_game_ids,
            "failed_game_details": [
                {
                    "game_id": failure.game_id,
                    "error_type": failure.error_type,
                    "message": failure.message,
                }
                for failure in error.failed_game_details
            ],
            "failure_reason_counts": error.failure_reason_counts,
            "failure_reason_examples": error.failure_reason_examples,
            "total_games": error.total_games,
            "failed_games": error.failed_games,
        }
    return {}


def _dataclass_fields(error: Exception, *, exclude: set[str]) -> dict[str, object]:
    if not is_dataclass(error):
        return {}
    return {key: value for key, value in asdict(error).items() if key not in exclude}


def _to_json_value(value: Any) -> Any:
    if isinstance(value, Season):
        return {
            "id": value.id,
            "start_year": value.start_year,
            "season_type": value.season_type.value,
        }
    if isinstance(value, Team):
        return {
            "team_id": value.team_id,
            "abbreviation": value.current.abbreviation,
        }
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _to_json_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): _to_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_to_json_value(item) for item in value]
    return value
