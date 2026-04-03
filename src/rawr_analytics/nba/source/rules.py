from __future__ import annotations

import json
import math
import re
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
_NUMERIC_TEXT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
_MINUTES_CLOCK_RE = re.compile(r"^(?P<minutes>\d+):(?P<seconds>\d+(?:\.\d+)?)$")
_ISO_DURATION_MINUTES_RE = re.compile(
    r"^PT(?:(?P<minutes>\d+(?:\.\d+)?)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?$"
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
    if not text or not _NUMERIC_TEXT_RE.fullmatch(text):
        return None
    numeric_value = float(text)
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
    if minute_text in {"0", "0:00", "0.0"}:
        return 0.0
    if ":" not in minute_text:
        if not _NUMERIC_TEXT_RE.fullmatch(minute_text):
            return None
        numeric_minutes = float(minute_text)
        if not math.isfinite(numeric_minutes):
            return None
        return numeric_minutes

    match = _MINUTES_CLOCK_RE.fullmatch(minute_text)
    if match is None:
        return None
    whole_minutes = float(match.group("minutes"))
    seconds = float(match.group("seconds"))
    parsed_minutes = whole_minutes + (seconds / 60.0)
    if not math.isfinite(parsed_minutes):
        return None
    return parsed_minutes


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
    if _is_skip_player_row(row):
        return PLAYER_DID_NOT_PLAY_PLACEHOLDER
    if _is_player_did_not_play_placeholder(row):
        return PLAYER_DID_NOT_PLAY_PLACEHOLDER
    if not source_player_played_in_game(row):
        return INACTIVE_PLAYER_STATUS_ROW
    return CANONICAL_PLAYER_SOURCE_ROW


def format_source_row(row: dict[str, object]) -> str:
    return json.dumps(row, sort_keys=True, default=str)


def format_source_rows(rows: list[dict[str, object]]) -> str:
    return json.dumps(rows, sort_keys=True, default=str)


def _parse_iso_duration_minutes(minute_text: str) -> float | None:
    match = _ISO_DURATION_MINUTES_RE.fullmatch(minute_text)
    if match is None:
        return None

    minute_part = match.group("minutes")
    second_part = match.group("seconds")
    if minute_part is None and second_part is None:
        return None
    minutes_value = float(minute_part) if minute_part is not None else 0.0
    seconds_value = float(second_part) if second_part is not None else 0.0
    parsed_minutes = minutes_value + (seconds_value / 60.0)
    if not math.isfinite(parsed_minutes):
        return None
    return parsed_minutes


def _played_in_game(minutes: object) -> bool:
    parsed_minutes = parse_minutes_to_float(minutes)
    if parsed_minutes is None:
        return False
    return parsed_minutes > 0.0


def source_player_played_in_game(row: SourceBoxScorePlayer) -> bool:
    has_stats = _row_has_any_box_score_stats(row.raw_row)
    has_minutes = _played_in_game(row.minutes_raw)
    return has_minutes or has_stats


def _is_skip_player_row(row: SourceBoxScorePlayer) -> bool:
    if row.player is not None and row.player.player_id not in {0}:
        return False
    player_name = "" if row.player is None else row.player.player_name.strip()
    return player_name == "" and row.minutes_raw in {
        None,
        "",
        0,
        "0",
        "0:00",
        "0.0",
    }


def _is_player_did_not_play_placeholder(row: SourceBoxScorePlayer) -> bool:
    if row.player is None or row.player.player_id <= 0:
        return False
    if row.player.player_name.strip() != "":
        return False
    return not source_player_played_in_game(row)


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
    for key in stat_keys:
        value = raw_row.get(key)
        if value is None:
            continue

        if isinstance(value, (int, float)):  # ints / floats
            if value != 0:
                return True
            continue
        if isinstance(value, str):  # strings like "0", "0.0"
            stripped = value.strip()
            if stripped == "":
                continue
            if stripped not in {"0", "0.0"}:
                return True
    return False


__all__ = [
    "CANONICAL_PLAYER_SOURCE_ROW",
    "CANONICAL_SCHEDULE_SOURCE_ROW",
    "CANONICAL_TEAM_SOURCE_ROW",
    "INACTIVE_PLAYER_STATUS_ROW",
    "PLAYER_DID_NOT_PLAY_PLACEHOLDER",
    "SourcePlayerRowClassification",
    "SourceScheduleRowClassification",
    "SourceTeamRowClassification",
    "_played_in_game",
    "classify_source_player_row",
    "classify_source_schedule_row",
    "classify_source_team_row",
    "format_source_row",
    "format_source_rows",
    "parse_box_score_numeric_value",
    "parse_minutes_to_float",
    "source_player_played_in_game",
]
