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

    report = run_regression(
        games_csv,
        game_players_csv,
        min_games=1,
        ridge_alpha=0.0,
    )

    assert "Regression results (Game-level player model)" in report
    assert "Player 101" in report


def test_run_regression_applies_top_n(tmp_path: Path):
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

    report = run_regression(
        games_csv,
        game_players_csv,
        min_games=1,
        ridge_alpha=0.0,
        top_n=1,
    )

    assert "Player 101" in report
    assert "Player 102" not in report


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
            "--ridge-alpha",
            "0.0",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Regression results (Game-level player model)" in captured.out


def test_main_runs_with_cached_scope_without_explicit_csvs(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
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
            "--team",
            "BOS",
            "--normalized-games-input-dir",
            str(normalized_games_dir),
            "--normalized-game-players-input-dir",
            str(normalized_players_dir),
            "--wowy-output-dir",
            str(tmp_path / "team_games"),
            "--combined-games-csv",
            str(tmp_path / "combined" / "games.csv"),
            "--combined-game-players-csv",
            str(tmp_path / "combined" / "game_players.csv"),
            "--source-data-dir",
            str(tmp_path / "source"),
            "--min-games",
            "1",
            "--ridge-alpha",
            "0.0",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Regression results (Game-level player model)" in captured.out


def test_main_rejects_negative_filters():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--min-games", "-1"])


def test_main_rejects_negative_ridge_alpha():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--ridge-alpha", "-1"])


def test_main_rejects_negative_top_n():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--top-n", "-1"])
