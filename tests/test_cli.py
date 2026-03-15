from __future__ import annotations

from pathlib import Path

import pytest

from wowy.apps.wowy.service import build_wowy_report, run_wowy
from wowy.main import main
from wowy.apps.wowy.models import WowyGameRecord


def test_main_runs_with_cached_scope_without_explicit_csv(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch,
):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-5,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,35.0\n"
            "1,BOS,102,Player 102,true,30.0\n"
            "2,BOS,102,Player 102,true,31.0\n"
            "2,BOS,103,Player 103,true,29.0\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("wowy.apps.wowy.cli.load_player_names_from_cache", lambda _: {})

    exit_code = main(
        [
            "--team",
            "BOS",
            "--normalized-games-input-dir",
            str(normalized_games_dir),
            "--normalized-game-players-input-dir",
            str(normalized_players_dir),
            "--wowy-output-dir",
            str(tmp_path / "team_games"),
            "--combined-wowy-csv",
            str(tmp_path / "combined" / "games.csv"),
            "--source-data-dir",
            str(tmp_path / "source"),
            "--min-games-with",
            "1",
            "--min-games-without",
            "1",
            "--min-average-minutes",
            "0",
            "--min-total-minutes",
            "0",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "WOWY results (Version 1)" in captured.out
    assert "avg_min" in captured.out


def test_main_filters_cached_scope_by_team_and_season(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch,
):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-5,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "NYK_2024-25.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "3,2024-25,2025-01-01,NYK,BOS,true,20,Regular Season,nba_api\n"
            "4,2024-25,2025-01-03,NYK,MIL,false,15,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,35.0\n"
            "2,BOS,102,Player 102,true,31.0\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "NYK_2024-25.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "3,NYK,201,Player 201,true,35.0\n"
            "4,NYK,201,Player 201,true,34.0\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("wowy.apps.wowy.cli.load_player_names_from_cache", lambda _: {})

    exit_code = main(
        [
            "--team",
            "BOS",
            "--season",
            "2023-24",
            "--normalized-games-input-dir",
            str(normalized_games_dir),
            "--normalized-game-players-input-dir",
            str(normalized_players_dir),
            "--wowy-output-dir",
            str(tmp_path / "team_games"),
            "--combined-wowy-csv",
            str(tmp_path / "combined" / "games.csv"),
            "--source-data-dir",
            str(tmp_path / "source"),
            "--min-games-with",
            "1",
            "--min-games-without",
            "1",
            "--min-average-minutes",
            "0",
            "--min-total-minutes",
            "0",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "101" in captured.out or "102" in captured.out
    assert "201" not in captured.out
    assert "avg_min" in captured.out


def test_run_wowy_returns_report_text(tmp_path: Path, write_games_csv):
    csv_path = tmp_path / "games.csv"
    write_games_csv(
        csv_path,
        [
            ["1", "2023-24", "team_1", "10", "101;102;103"],
            ["2", "2023-24", "team_1", "0", "102;103;104"],
            ["3", "2023-24", "team_1", "-10", "103;104;105"],
        ],
    )

    report = run_wowy(
        csv_path,
        min_games_with=1,
        min_games_without=1,
        player_names={101: "Player 101"},
    )

    assert "WOWY results (Version 1)" in report
    assert "Player 101" in report
    assert "avg_min" in report
    assert "tot_min" in report


def test_run_wowy_applies_top_n(tmp_path: Path, write_games_csv):
    csv_path = tmp_path / "games.csv"
    write_games_csv(
        csv_path,
        [
            ["1", "2023-24", "team_1", "10", "101;102"],
            ["2", "2023-24", "team_1", "0", "101"],
            ["3", "2023-24", "team_1", "-10", "102"],
        ],
    )

    report = run_wowy(
        csv_path,
        min_games_with=1,
        min_games_without=1,
        player_names={101: "Player 101", 102: "Player 102"},
        top_n=1,
    )

    assert "Player 101" in report
    assert "Player 102" not in report
    assert "avg_min" in report
    assert "tot_min" in report


def test_run_wowy_applies_minutes_filters(tmp_path: Path, write_games_csv):
    csv_path = tmp_path / "games.csv"
    write_games_csv(
        csv_path,
        [
            ["1", "2023-24", "team_1", "10", "101;102"],
            ["2", "2023-24", "team_1", "0", "101"],
            ["3", "2023-24", "team_1", "-10", "102"],
        ],
    )

    report = run_wowy(
        csv_path,
        min_games_with=1,
        min_games_without=1,
        player_names={101: "Player 101", 102: "Player 102"},
        player_minute_stats={
            101: (28.0, 56.0),
            102: (12.0, 24.0),
        },
        min_average_minutes=20.0,
        min_total_minutes=40.0,
    )

    assert "Player 101" in report
    assert "Player 102" not in report


def test_main_rejects_negative_filters():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--min-games-with", "-1"])


def test_main_rejects_negative_top_n():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--top-n", "-1"])


def test_build_wowy_report_formats_preloaded_games():
    games: list[WowyGameRecord] = [
        WowyGameRecord("1", "2023-24", "team_1", 10.0, {101, 102, 103}),
        WowyGameRecord("2", "2023-24", "team_1", -5.0, {102, 103, 104}),
    ]

    report = build_wowy_report(
        games,
        min_games_with=1,
        min_games_without=1,
        player_names={101: "Player 101"},
    )

    assert "WOWY results (Version 1)" in report
    assert "Player 101" in report


def test_main_filters_cached_scope_by_minutes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch,
):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-5,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,8,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,35.0\n"
            "1,BOS,102,Player 102,true,10.0\n"
            "2,BOS,101,Player 101,true,33.0\n"
            "3,BOS,102,Player 102,true,12.0\n"
            "3,BOS,103,Player 103,true,28.0\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("wowy.apps.wowy.cli.load_player_names_from_cache", lambda _: {})

    exit_code = main(
        [
            "--team",
            "BOS",
            "--normalized-games-input-dir",
            str(normalized_games_dir),
            "--normalized-game-players-input-dir",
            str(normalized_players_dir),
            "--wowy-output-dir",
            str(tmp_path / "team_games"),
            "--combined-wowy-csv",
            str(tmp_path / "combined" / "games.csv"),
            "--source-data-dir",
            str(tmp_path / "source"),
            "--min-games-with",
            "1",
            "--min-games-without",
            "1",
            "--min-average-minutes",
            "20",
            "--min-total-minutes",
            "60",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "101" in captured.out
    assert "102" not in captured.out
    assert "34.0" in captured.out
    assert "68.0" in captured.out


def test_help_works(capsys: pytest.CaptureFixture[str]):
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "Run WOWY on cached data" in captured.out
