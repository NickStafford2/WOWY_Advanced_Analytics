from __future__ import annotations

from pathlib import Path

import pytest

from wowy.regression_cli import main, run_regression


def test_run_regression_returns_report_text(tmp_path: Path):
    games_csv = tmp_path / "games.csv"
    games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_csv = tmp_path / "game_players.csv"
    game_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,\n"
            "2,BOS,102,Player 102,true,\n"
            "3,BOS,101,Player 101,true,\n"
            "3,BOS,102,Player 102,true,\n"
        ),
        encoding="utf-8",
    )

    report = run_regression(games_csv, game_players_csv, min_games=1)

    assert "Regression results (Game-level player model)" in report
    assert "Player 101" in report


def test_main_runs_with_temp_csvs(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
):
    games_csv = tmp_path / "games.csv"
    games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_csv = tmp_path / "game_players.csv"
    game_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,\n"
            "2,BOS,102,Player 102,true,\n"
            "3,BOS,101,Player 101,true,\n"
            "3,BOS,102,Player 102,true,\n"
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--games-csv",
            str(games_csv),
            "--game-players-csv",
            str(game_players_csv),
            "--min-games",
            "1",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Regression results (Game-level player model)" in captured.out


def test_main_rejects_negative_filters():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--min-games", "-1"])
