from __future__ import annotations

import json
import math
from dataclasses import dataclass

from rawr_analytics.nba.source.models import SourceBoxScorePlayer, SourceBoxScoreTeam


@dataclass(frozen=True)
class SourcePlayerRowClassification:
    kind: str
    should_skip: bool = False


@dataclass(frozen=True)
class SourceTeamRowClassification:
    kind: str
    should_skip: bool = False


@dataclass(frozen=True)
class SourceScheduleRowClassification:
    kind: str
    should_skip: bool = False


PLAYER_DID_NOT_PLAY_PLACEHOLDER = SourcePlayerRowClassification(
    kind="player_did_not_play_placeholder",
    should_skip=True,
)
INACTIVE_PLAYER_STATUS_ROW = SourcePlayerRowClassification(
    kind="inactive_player_status_row",
    should_skip=True,
)
CANONICAL_PLAYER_SOURCE_ROW = SourcePlayerRowClassification(
    kind="canonical_player_source_row",
    should_skip=False,
)
CANONICAL_TEAM_SOURCE_ROW = SourceTeamRowClassification(
    kind="canonical_team_source_row",
    should_skip=False,
)
CANONICAL_SCHEDULE_SOURCE_ROW = SourceScheduleRowClassification(
    kind="canonical_schedule_source_row",
    should_skip=False,
)


def parse_box_score_numeric_value(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            return None
        return numeric_value

    text = str(value).strip()
    if not text:
        return None
    try:
        numeric_value = float(text)
    except ValueError:
        return None
    if not math.isfinite(numeric_value):
        return None
    return numeric_value


def parse_minutes_to_float(minutes: object) -> float | None:
    if minutes is None or isinstance(minutes, bool):
        return None
    if isinstance(minutes, int | float):
        numeric_minutes = float(minutes)
        if not math.isfinite(numeric_minutes):
            return None
        return numeric_minutes

    minute_text = str(minutes).strip()
    if not minute_text:
        return None
    if minute_text.startswith("PT"):
        return _parse_iso_duration_minutes(minute_text)
    if _is_known_inactive_status(minute_text):
        return None
    if minute_text in {"0", "0:00", "0.0"}:
        return 0.0
    if ":" not in minute_text:
        try:
            numeric_minutes = float(minute_text)
        except ValueError:
            return None
        if not math.isfinite(numeric_minutes):
            return None
        return numeric_minutes

    whole_minutes, seconds = minute_text.split(":", maxsplit=1)
    try:
        parsed_minutes = float(whole_minutes) + (float(seconds) / 60.0)
    except ValueError:
        return None
    if not math.isfinite(parsed_minutes):
        return None
    return parsed_minutes


def played_in_game(minutes: object) -> bool:
    parsed_minutes = parse_minutes_to_float(minutes)
    if parsed_minutes is None:
        return False
    return parsed_minutes > 0.0


# todo: find out why I made this.
def classify_source_team_row(
    row: SourceBoxScoreTeam,
) -> SourceTeamRowClassification:
    return CANONICAL_TEAM_SOURCE_ROW


# todo: find out why I made this.
def classify_source_schedule_row(
    row: dict[str, object],
) -> SourceScheduleRowClassification:
    return CANONICAL_SCHEDULE_SOURCE_ROW


def classify_source_player_row(
    row: SourceBoxScorePlayer,
) -> SourcePlayerRowClassification:
    if _is_inactive_player_status_row(row):
        return INACTIVE_PLAYER_STATUS_ROW
    if _is_skip_player_row(row):
        return PLAYER_DID_NOT_PLAY_PLACEHOLDER
    if _is_player_did_not_play_placeholder(row):
        return PLAYER_DID_NOT_PLAY_PLACEHOLDER
    return CANONICAL_PLAYER_SOURCE_ROW


def format_source_row(row: dict[str, object]) -> str:
    return json.dumps(row, sort_keys=True, default=str)


def format_source_rows(rows: list[dict[str, object]]) -> str:
    return json.dumps(rows, sort_keys=True, default=str)


def _parse_iso_duration_minutes(minute_text: str) -> float | None:
    body = minute_text.removeprefix("PT")
    if not body:
        return None

    minutes_value = 0.0
    seconds_value = 0.0
    if "M" in body:
        minute_part, body = body.split("M", maxsplit=1)
        if minute_part:
            try:
                minutes_value = float(minute_part)
            except ValueError:
                return None
    if body.endswith("S"):
        second_part = body[:-1]
        if second_part:
            try:
                seconds_value = float(second_part)
            except ValueError:
                return None

    parsed_minutes = minutes_value + (seconds_value / 60.0)
    if not math.isfinite(parsed_minutes):
        return None
    return parsed_minutes


def _is_skip_player_row(row: SourceBoxScorePlayer) -> bool:
    if row.player_id not in {None, 0}:
        return False
    return row.player_name.strip() == "" and row.minutes_raw in {
        None,
        "",
        0,
        "0",
        "0:00",
        "0.0",
    }


def _is_player_did_not_play_placeholder(row: SourceBoxScorePlayer) -> bool:
    if row.player_id is None or row.player_id <= 0:
        return False
    if row.player_name.strip() != "":
        return False
    if row.minutes_raw is not None:
        return False
    # You can Definitely reach here. I don't know why pyright thinks otherwise
    # print("pyright thinks you can't reach here.")
    if _row_has_any_box_score_stats(row.raw_row):
        return False
    comment = str(row.raw_row.get("COMMENT", "")).strip().upper()
    if comment not in {"", "DNP - COACH'S DECISION"}:
        return False
    return True


def _is_inactive_player_status_row(row: SourceBoxScorePlayer) -> bool:
    if _row_has_any_box_score_stats(row.raw_row):
        return False

    minute_text = str(row.minutes_raw).strip() if row.minutes_raw is not None else ""
    comment = str(row.raw_row.get("COMMENT", "")).strip()
    if minute_text and _is_known_inactive_status(minute_text):
        return True
    if comment and _is_known_inactive_status(comment):
        return True
    if row.player_id is None or row.player_id <= 0 or row.player_name.strip() == "":
        return False
    if minute_text and _is_known_inactive_status(minute_text):
        return True
    if minute_text == "" and comment == "":
        return True
    if not comment:
        return False
    return _is_known_inactive_status(comment)


def _row_has_any_box_score_stats(raw_row: dict[str, object]) -> bool:
    stat_keys = (
        "AST",
        "BLK",
        "DREB",
        "FG3A",
        "FG3M",
        "FG3_PCT",
        "FGA",
        "FGM",
        "FG_PCT",
        "FTA",
        "FTM",
        "FT_PCT",
        "OREB",
        "PF",
        "PLUS_MINUS",
        "PTS",
        "REB",
        "STL",
        "TO",
    )
    return any(raw_row.get(key) is not None for key in stat_keys)


def _is_known_inactive_status(minute_text: str) -> bool:
    normalized = minute_text.strip().upper()
    inactive_prefixes = (
        "DNP",
        "DNA",
        "DND",
        "DWT",
        "PLANTAR FASCIATIS",
        "DNT",
        "DN MAKE TRIP",
        "MWT",
        "NWT",
        "NTW",
        "NOT DRESS",
        "HAMSTRING",
        "DID NOT",
        "WILL NOT PLAY",
        "OUT",
        "INJ",
        "NBA SUSPENSION",
        "NOT WITH TEAM",
        "SUSPENDED",
        "INACTIVE",
    )
    return normalized.startswith(inactive_prefixes)


__all__ = [
    "CANONICAL_PLAYER_SOURCE_ROW",
    "CANONICAL_SCHEDULE_SOURCE_ROW",
    "CANONICAL_TEAM_SOURCE_ROW",
    "INACTIVE_PLAYER_STATUS_ROW",
    "PLAYER_DID_NOT_PLAY_PLACEHOLDER",
    "SourcePlayerRowClassification",
    "SourceScheduleRowClassification",
    "SourceTeamRowClassification",
    "classify_source_player_row",
    "classify_source_schedule_row",
    "classify_source_team_row",
    "format_source_row",
    "format_source_rows",
    "parse_box_score_numeric_value",
    "parse_minutes_to_float",
    "played_in_game",
]
