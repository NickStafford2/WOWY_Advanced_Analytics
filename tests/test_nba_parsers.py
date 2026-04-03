from __future__ import annotations

import json
from pathlib import Path

import pytest

from rawr_analytics.basketball.nba_api import load_player_names_from_cache
from rawr_analytics.basketball.nba_api.parsers import (
    parse_box_score_payload,
    parse_league_schedule_payload,
)


def test_parse_box_score_payload_accepts_live_game_shape() -> None:
    box_score = parse_box_score_payload(
        {
            "game": {
                "homeTeam": {
                    "teamId": 1610612763,
                    "teamTricode": "MEM",
                    "score": 100,
                    "statistics": {"plusMinusPoints": 5},
                    "players": [
                        {
                            "personId": 1,
                            "firstName": "Mike",
                            "familyName": "Miller",
                            "statistics": {"minutes": "PT30M15S"},
                        }
                    ],
                },
                "awayTeam": {
                    "teamId": 1610612738,
                    "teamTricode": "BOS",
                    "score": 95,
                    "statistics": {"plusMinusPoints": -5},
                    "players": [
                        {
                            "personId": 10,
                            "firstName": "Paul",
                            "familyName": "Pierce",
                            "statistics": {"minutes": "PT32M00S"},
                        }
                    ],
                },
            }
        },
        game_id="0001",
    )

    assert len(box_score.teams) == 2
    assert box_score.players[0].player_name == "Mike Miller"
    assert box_score.players[0].team_abbreviation == "MEM"


def test_parse_box_score_payload_prefers_live_minutes_calculated() -> None:
    box_score = parse_box_score_payload(
        {
            "game": {
                "homeTeam": {
                    "teamId": 1610612763,
                    "teamTricode": "MEM",
                    "score": 100,
                    "statistics": {"plusMinusPoints": 5},
                    "players": [
                        {
                            "personId": 1,
                            "firstName": "Mike",
                            "familyName": "Miller",
                            "statistics": {
                                "minutes": "PT30M15S",
                                "minutesCalculated": "PT31M",
                            },
                        }
                    ],
                },
                "awayTeam": {
                    "teamId": 1610612738,
                    "teamTricode": "BOS",
                    "score": 95,
                    "statistics": {"plusMinusPoints": -5},
                    "players": [
                        {
                            "personId": 10,
                            "firstName": "Paul",
                            "familyName": "Pierce",
                            "statistics": {"minutes": "PT32M00S"},
                        }
                    ],
                },
            }
        },
        game_id="0001",
    )

    assert box_score.players[0].minutes_raw == "PT31M"
    assert box_score.players[1].minutes_raw == "PT32M00S"


def test_parse_box_score_payload_uses_alias_keys_in_v3_rows() -> None:
    box_score = parse_box_score_payload(
        {
            "resultSets": {
                "PlayerStats": {
                    "headers": [
                        "gameId",
                        "teamId",
                        "teamTricode",
                        "personId",
                        "firstName",
                        "familyName",
                        "minutes",
                    ],
                    "data": [["0001", 1610612763, "MEM", 1, "Mike", "Miller", "PT30M00S"]],
                },
                "TeamStats": {
                    "headers": ["teamId", "teamTricode", "plusMinusPoints", "points"],
                    "data": [[1610612763, "MEM", 5, 100], [1610612738, "BOS", -5, 95]],
                },
            }
        },
        game_id="0001",
    )

    assert box_score.players[0].game_id == "0001"
    assert box_score.players[0].player_name == "Mike Miller"
    assert box_score.teams[0].team_abbreviation == "MEM"


def test_parse_league_schedule_payload_canonicalizes_scope_metadata() -> None:
    schedule = parse_league_schedule_payload(
        {
            "resultSets": [
                {
                    "headers": ["GAME_ID", "GAME_DATE", "MATCHUP", "TEAM_ID", "TEAM_ABBREVIATION"],
                    "rowSet": [["0001", "2003-03-10", "MEM vs. BOS", 1610612763, "MEM"]],
                }
            ]
        },
        requested_team=" mem ",
        season="2002-03",
        season_type="regular season",
    )

    assert schedule.requested_team == "MEM"
    assert schedule.season == "2002-03"
    assert schedule.season_type == "Regular Season"


def test_parse_box_score_payload_raises_for_missing_result_sets() -> None:
    with pytest.raises(ValueError, match="missing resultSets"):
        parse_box_score_payload({}, game_id="0001")


def test_load_player_names_from_cache_reads_valid_payloads_only(tmp_path: Path) -> None:
    boxscores_dir = tmp_path / "boxscores"
    boxscores_dir.mkdir(parents=True, exist_ok=True)
    (boxscores_dir / "0001_boxscoretraditionalv2.json").write_text(
        json.dumps(
            {
                "resultSets": [
                    {
                        "name": "PlayerStats",
                        "headers": [
                            "GAME_ID",
                            "TEAM_ID",
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                        ],
                        "rowSet": [["0001", 1610612763, "MEM", 1, "Mike Miller", "12:00"]],
                    },
                    {
                        "name": "TeamStats",
                        "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                        "rowSet": [[1610612763, "MEM", 5, 100], [1610612738, "BOS", -5, 95]],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    (boxscores_dir / "0002_boxscoretraditionalv2.json").write_text("{", encoding="utf-8")

    assert load_player_names_from_cache(tmp_path) == {1: "Mike Miller"}


def test_load_player_names_from_cache_discards_empty_and_fails_fast_on_invalid_payloads(
    tmp_path: Path,
) -> None:
    boxscores_dir = tmp_path / "boxscores"
    boxscores_dir.mkdir(parents=True, exist_ok=True)
    empty_path = boxscores_dir / "0001_boxscoretraditionalv2.json"
    empty_path.write_text(
        json.dumps(
            {
                "resultSets": [
                    {
                        "name": "PlayerStats",
                        "headers": [
                            "GAME_ID",
                            "TEAM_ID",
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                        ],
                        "rowSet": [],
                    },
                    {
                        "name": "TeamStats",
                        "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                        "rowSet": [],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    valid_path = boxscores_dir / "0002_boxscoretraditionalv2.json"
    valid_path.write_text(
        json.dumps(
            {
                "resultSets": [
                    {
                        "name": "PlayerStats",
                        "headers": [
                            "GAME_ID",
                            "TEAM_ID",
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                        ],
                        "rowSet": [["0002", 1610612763, "MEM", 3, "Good Player", "12:00"]],
                    },
                    {
                        "name": "TeamStats",
                        "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                        "rowSet": [[1610612763, "MEM", 5, 100], [1610612738, "BOS", -5, 95]],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    invalid_path = boxscores_dir / "9999_boxscoretraditionalv2.json"
    invalid_path.write_text(
        json.dumps(
            {
                "resultSets": [
                    {
                        "name": "PlayerStats",
                        "headers": [
                            "GAME_ID",
                            "TEAM_ID",
                            "TEAM_ABBREVIATION",
                            "PLAYER_ID",
                            "PLAYER_NAME",
                            "MIN",
                        ],
                        "rowSet": [["0002", 1610612763, "MEM", 2, "Broken Player", "12:00"]],
                    },
                    {
                        "name": "TeamStats",
                        "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS"],
                        "rowSet": [[1610612763, "MEM", "not-a-number"]],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    logs: list[str] = []

    with pytest.raises(ValueError, match="Invalid PLUS_MINUS value"):
        load_player_names_from_cache(tmp_path, log=logs.append)

    assert not empty_path.exists()
    assert valid_path.exists()
    assert any(
        "cache discard" in message and "invalid_or_empty_payload" in message for message in logs
    )
