from __future__ import annotations

import json
from pathlib import Path

from rawr_analytics.basketball.nba_api_audit import _audit_nba_api
from rawr_analytics.basketball.nba_api_audit import main as source_audit_main


def test_audit_nba_api_reports_known_classifications_without_failure(tmp_path: Path) -> None:
    _write_schedule_payload(
        tmp_path / "team_seasons" / "MEM_2002-03_regular_season_leaguegamefinder.json",
        rows=[["0001", "2003-03-10", "MEM vs. BOS", 1610612763, "MEM"]],
    )
    _write_box_score_payload(
        tmp_path / "boxscores" / "0001_boxscoretraditionalv2.json",
        player_rows=[
            [
                "0001",
                1610612763,
                "MEM",
                1337,
                None,
                None,
                " ",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "0001",
                1610612763,
                "MEM",
                2001,
                "Inactive Player",
                None,
                "DNP - Coach's Decision",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "0001",
                1610612763,
                "MEM",
                1,
                "P1",
                "48:00",
                "",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "0001",
                1610612738,
                "BOS",
                10,
                "Opp",
                "48:00",
                "",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ],
        team_rows=[
            [1610612763, "MEM", 5, 100],
            [1610612738, "BOS", -5, 95],
        ],
    )

    report = _audit_nba_api(tmp_path)

    assert report.ok is True
    assert report.classification_counts == {
        "inactive_player_status_row": 1,
        "player_did_not_play_placeholder": 1,
    }
    assert report.failure_counts == {}


def test_source_audit_cli_returns_nonzero_for_hard_failures(tmp_path: Path, capsys) -> None:
    _write_schedule_payload(
        tmp_path / "team_seasons" / "MEM_2002-03_regular_season_leaguegamefinder.json",
        rows=[["0001", "2003-03-10", "MEM vs. BOS", 1610612763, "MEM"]],
    )
    _write_box_score_payload(
        tmp_path / "boxscores" / "0001_boxscoretraditionalv2.json",
        player_rows=[
            [
                "0001",
                1610612763,
                "MEM",
                1,
                "",
                "48:00",
                "",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "0001",
                1610612738,
                "BOS",
                10,
                "Opp",
                "48:00",
                "",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ],
        team_rows=[
            [1610612763, "MEM", 5, 100],
            [1610612738, "BOS", -5, 95],
        ],
    )

    exit_code = source_audit_main(["--source-dir", str(tmp_path)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Source audit status: invalid" in captured.out
    assert "box_score_error" in captured.out
    assert "Missing PLAYER_NAME" in captured.out


def _write_schedule_payload(path: Path, *, rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "resultSets": [
            {
                "headers": [
                    "GAME_ID",
                    "GAME_DATE",
                    "MATCHUP",
                    "TEAM_ID",
                    "TEAM_ABBREVIATION",
                ],
                "rowSet": rows,
            }
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_box_score_payload(
    path: Path,
    *,
    player_rows: list[list[object]],
    team_rows: list[list[object]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
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
                    "COMMENT",
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
                ],
                "rowSet": player_rows,
            },
            {
                "name": "TeamStats",
                "headers": ["TEAM_ID", "TEAM_ABBREVIATION", "PLUS_MINUS", "PTS"],
                "rowSet": team_rows,
            },
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
