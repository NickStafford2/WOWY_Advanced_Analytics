from __future__ import annotations

import math

import pytest

from wowy.nba.source_models import SourceBoxScorePlayer, SourceBoxScoreTeam
from wowy.nba.source_rules import (
    CANONICAL_PLAYER_SOURCE_ROW,
    CANONICAL_SCHEDULE_SOURCE_ROW,
    CANONICAL_TEAM_SOURCE_ROW,
    INACTIVE_PLAYER_STATUS_ROW,
    PLAYER_DID_NOT_PLAY_PLACEHOLDER,
    classify_source_player_row,
    classify_source_schedule_row,
    classify_source_team_row,
    parse_box_score_numeric_value,
    parse_minutes_to_float,
    played_in_game,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("48:00", 48.0),
        ("PT47M30S", 47.5),
        ("12.5", 12.5),
        ("0:00", 0.0),
        ("DNP - Coach's Decision", None),
        (None, None),
        ("bad", None),
    ],
)
def test_parse_minutes_to_float_handles_expected_source_formats(
    value: object,
    expected: float | None,
) -> None:
    assert parse_minutes_to_float(value) == expected


def test_played_in_game_only_treats_positive_minutes_as_appearance() -> None:
    assert played_in_game("12:00") is True
    assert played_in_game("0:00") is False
    assert played_in_game("DND - Injury") is False


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (5, 5.0),
        ("-3.5", -3.5),
        ("", None),
        (True, None),
        (math.inf, None),
        ("nan", None),
    ],
)
def test_parse_box_score_numeric_value_filters_invalid_inputs(
    value: object,
    expected: float | None,
) -> None:
    assert parse_box_score_numeric_value(value) == expected


def test_classify_source_player_row_names_all_known_skip_patterns() -> None:
    sentinel = SourceBoxScorePlayer(
        game_id="0001",
        team_id=1610612763,
        team_abbreviation="MEM",
        player_id=0,
        player_name="",
        minutes_raw="0:00",
        raw_row={},
    )
    placeholder = SourceBoxScorePlayer(
        game_id="0001",
        team_id=1610612763,
        team_abbreviation="MEM",
        player_id=1337,
        player_name="",
        minutes_raw=None,
        raw_row={"COMMENT": " "},
    )
    inactive = SourceBoxScorePlayer(
        game_id="0001",
        team_id=1610612763,
        team_abbreviation="MEM",
        player_id=2001,
        player_name="Inactive Player",
        minutes_raw=None,
        raw_row={"COMMENT": "DNP - Coach's Decision"},
    )
    canonical = SourceBoxScorePlayer(
        game_id="0001",
        team_id=1610612763,
        team_abbreviation="MEM",
        player_id=1,
        player_name="P1",
        minutes_raw="12:00",
        raw_row={"COMMENT": ""},
    )

    assert classify_source_player_row(sentinel) == PLAYER_DID_NOT_PLAY_PLACEHOLDER
    assert classify_source_player_row(placeholder) == PLAYER_DID_NOT_PLAY_PLACEHOLDER
    assert classify_source_player_row(inactive) == INACTIVE_PLAYER_STATUS_ROW
    assert classify_source_player_row(canonical) == CANONICAL_PLAYER_SOURCE_ROW


def test_classify_source_team_and_schedule_rows_are_canonical_by_default() -> None:
    team_row = SourceBoxScoreTeam(
        team_id=1610612763,
        team_abbreviation="MEM",
        plus_minus_raw=5,
        points_raw=100,
        raw_row={},
    )

    assert classify_source_team_row(team_row) == CANONICAL_TEAM_SOURCE_ROW
    assert classify_source_schedule_row({"GAME_ID": "0001"}) == CANONICAL_SCHEDULE_SOURCE_ROW
