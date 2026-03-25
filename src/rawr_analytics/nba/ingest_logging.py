from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from wowy.nba.errors import (
    FetchError,
    PartialTeamSeasonError,
)

DEFAULT_INGEST_FAILURE_LOG_PATH = Path("data/logs/ingest_failures.jsonl")


def append_ingest_failure_log(
    *,
    team: str,
    season: str,
    season_type: str,
    failure_kind: str,
    error: Exception,
    log_path: Path = DEFAULT_INGEST_FAILURE_LOG_PATH,
) -> None:
    record: dict[str, Any] = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "team": team,
        "season": season,
        "season_type": season_type,
        "failure_kind": failure_kind,
        "error_type": type(error).__name__,
        "message": str(error),
    }
    record.update(_build_error_details(error))

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(record, sort_keys=True))
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
            "season_type_scope": error.season_type,
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
    return {
        key: value
        for key, value in asdict(error).items()
        if key not in exclude
    }
