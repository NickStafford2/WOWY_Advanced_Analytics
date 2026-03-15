from __future__ import annotations

from pathlib import Path

import pytest

from wowy.all_cached_cli import main, run_all_cached


def test_run_all_cached_combines_all_inputs_and_returns_both_reports(tmp_path: Path):
    wowy_input_dir = tmp_path / "team_games"
    wowy_input_dir.mkdir()
    (wowy_input_dir / "BOS_2023-24.csv").write_text(
        "game_id,team,margin,players\n"
        '1,BOS,10,"101;102"\n'
        '2,BOS,-2,"101"\n',
        encoding="utf-8",
    )
    (wowy_input_dir / "NYK_2023-24.csv").write_text(
        "game_id,team,margin,players\n"
        '3,NYK,4,"201;202"\n'
        '4,NYK,-1,"201"\n',
        encoding="utf-8",
    )

    normalized_games_input_dir = tmp_path / "normalized_games"
    normalized_games_input_dir.mkdir()
    (normalized_games_input_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    normalized_game_players_input_dir = tmp_path / "normalized_game_players"
    normalized_game_players_input_dir.mkdir()
    (normalized_game_players_input_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,\n"
            "2,BOS,102,Player 102,true,\n"
            "3,BOS,101,Player 101,true,\n"
            "3,BOS,102,Player 102,true,\n"
        ),
        encoding="utf-8",
    )

    report = run_all_cached(
        wowy_input_dir=wowy_input_dir,
        normalized_games_input_dir=normalized_games_input_dir,
        normalized_game_players_input_dir=normalized_game_players_input_dir,
        combined_wowy_csv=tmp_path / "combined" / "wowy.csv",
        combined_regression_games_csv=tmp_path / "combined" / "reg_games.csv",
        combined_regression_game_players_csv=tmp_path / "combined" / "reg_players.csv",
        source_data_dir=tmp_path / "source-data",
        min_games_with=1,
        min_games_without=1,
        wowy_top_n=None,
        min_regression_games=1,
        ridge_alpha=0.0,
        regression_top_n=None,
    )

    assert "WOWY results (Version 1)" in report
    assert "Regression results (Game-level player model)" in report


def test_main_runs_with_temp_cached_directories(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
):
    wowy_input_dir = tmp_path / "team_games"
    wowy_input_dir.mkdir()
    (wowy_input_dir / "BOS_2023-24.csv").write_text(
        "game_id,team,margin,players\n"
        '1,BOS,10,"101;102"\n'
        '2,BOS,-2,"101"\n',
        encoding="utf-8",
    )

    normalized_games_input_dir = tmp_path / "normalized_games"
    normalized_games_input_dir.mkdir()
    (normalized_games_input_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    normalized_game_players_input_dir = tmp_path / "normalized_game_players"
    normalized_game_players_input_dir.mkdir()
    (normalized_game_players_input_dir / "BOS_2023-24.csv").write_text(
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
            "--wowy-input-dir",
            str(wowy_input_dir),
            "--normalized-games-input-dir",
            str(normalized_games_input_dir),
            "--normalized-game-players-input-dir",
            str(normalized_game_players_input_dir),
            "--combined-wowy-csv",
            str(tmp_path / "combined" / "wowy.csv"),
            "--combined-regression-games-csv",
            str(tmp_path / "combined" / "reg_games.csv"),
            "--combined-regression-game-players-csv",
            str(tmp_path / "combined" / "reg_players.csv"),
            "--min-games-with",
            "1",
            "--min-games-without",
            "1",
            "--min-regression-games",
            "1",
            "--ridge-alpha",
            "0.0",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "WOWY results (Version 1)" in captured.out
    assert "Regression results (Game-level player model)" in captured.out
