from __future__ import annotations

from pathlib import Path

import pytest

from wowy.apps.rawr.cli import main

from wowy.apps.rawr.service import parse_ridge_grid, run_rawr


def test_run_rawr_returns_report_text(tmp_path: Path):
    games_csv = tmp_path / "games.csv"
    games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "1,2023-24,2024-04-01,MIL,BOS,false,-2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,NYK,BOS,true,2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,LAL,BOS,false,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_csv = tmp_path / "game_players.csv"
    game_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,48\n"
            "1,MIL,201,Player 201,true,48\n"
            "2,BOS,102,Player 102,true,48\n"
            "2,NYK,202,Player 202,true,48\n"
            "3,BOS,101,Player 101,true,24\n"
            "3,BOS,102,Player 102,true,24\n"
            "3,LAL,201,Player 201,true,24\n"
            "3,LAL,202,Player 202,true,24\n"
        ),
        encoding="utf-8",
    )

    report = run_rawr(
        games_csv,
        game_players_csv,
        min_games=1,
        ridge_alpha=1.0,
    )

    assert "RAWR results (Game-level player model)" in report
    assert "Player 101" in report


def test_run_rawr_applies_top_n(tmp_path: Path):
    games_csv = tmp_path / "games.csv"
    games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "1,2023-24,2024-04-01,MIL,BOS,false,-2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,NYK,BOS,true,2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,LAL,BOS,false,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_csv = tmp_path / "game_players.csv"
    game_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,48\n"
            "1,MIL,201,Player 201,true,48\n"
            "2,BOS,102,Player 102,true,48\n"
            "2,NYK,202,Player 202,true,48\n"
            "3,BOS,101,Player 101,true,24\n"
            "3,BOS,102,Player 102,true,24\n"
            "3,LAL,201,Player 201,true,24\n"
            "3,LAL,202,Player 202,true,24\n"
        ),
        encoding="utf-8",
    )

    report = run_rawr(
        games_csv,
        game_players_csv,
        min_games=1,
        ridge_alpha=1.0,
        top_n=1,
    )

    assert len(report.splitlines()) == 6


def test_run_rawr_applies_minute_filters(tmp_path: Path):
    games_csv = tmp_path / "games.csv"
    games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "1,2023-24,2024-04-01,MIL,BOS,false,-2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,NYK,BOS,true,2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,LAL,BOS,false,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_csv = tmp_path / "game_players.csv"
    game_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,30\n"
            "1,BOS,102,Player 102,true,10\n"
            "1,MIL,201,Player 201,true,48\n"
            "2,BOS,101,Player 101,true,30\n"
            "2,BOS,102,Player 102,true,10\n"
            "2,NYK,202,Player 202,true,48\n"
            "3,BOS,101,Player 101,true,30\n"
            "3,BOS,102,Player 102,true,10\n"
            "3,LAL,203,Player 203,true,48\n"
        ),
        encoding="utf-8",
    )

    report = run_rawr(
        games_csv,
        game_players_csv,
        min_games=1,
        ridge_alpha=1.0,
        player_minute_stats={
            ("2023-24", 101): (30.0, 90.0),
            ("2023-24", 102): (10.0, 30.0),
            ("2023-24", 201): (48.0, 48.0),
            ("2023-24", 202): (48.0, 48.0),
            ("2023-24", 203): (48.0, 48.0),
        },
        min_average_minutes=20.0,
        min_total_minutes=40.0,
    )

    assert "Player 101" in report
    assert "Player 102" not in report


def test_run_rawr_multi_season_output_separates_player_seasons(tmp_path: Path):
    games_csv = tmp_path / "games.csv"
    games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2022-23,2023-04-01,BOS,MIL,true,6,Regular Season,nba_api\n"
            "1,2022-23,2023-04-01,MIL,BOS,false,-6,Regular Season,nba_api\n"
            "2,2022-23,2023-04-03,BOS,NYK,true,5,Regular Season,nba_api\n"
            "2,2022-23,2023-04-03,NYK,BOS,false,-5,Regular Season,nba_api\n"
            "3,2023-24,2024-04-01,BOS,MIL,true,-6,Regular Season,nba_api\n"
            "3,2023-24,2024-04-01,MIL,BOS,false,6,Regular Season,nba_api\n"
            "4,2023-24,2024-04-03,BOS,NYK,true,-5,Regular Season,nba_api\n"
            "4,2023-24,2024-04-03,NYK,BOS,false,5,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_csv = tmp_path / "game_players.csv"
    game_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,48\n"
            "1,MIL,201,Player 201,true,48\n"
            "2,BOS,101,Player 101,true,48\n"
            "2,NYK,202,Player 202,true,48\n"
            "3,BOS,101,Player 101,true,48\n"
            "3,MIL,201,Player 201,true,48\n"
            "4,BOS,101,Player 101,true,48\n"
            "4,NYK,202,Player 202,true,48\n"
        ),
        encoding="utf-8",
    )

    report = run_rawr(
        games_csv,
        game_players_csv,
        min_games=2,
        ridge_alpha=1.0,
    )

    assert "2022-23" in report
    assert "2023-24" in report
    assert report.count("Player 101") == 2


def test_run_rawr_team_scope_applies_minute_filters_after_fit(tmp_path: Path):
    games_csv = tmp_path / "games.csv"
    games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,2,Regular Season,nba_api\n"
            "1,2023-24,2024-04-01,MIL,BOS,false,-2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-2,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,NYK,BOS,true,2,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,BOS,LAL,true,0,Regular Season,nba_api\n"
            "3,2023-24,2024-04-05,LAL,BOS,false,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_csv = tmp_path / "game_players.csv"
    game_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,30\n"
            "1,BOS,102,Player 102,true,10\n"
            "1,MIL,201,Player 201,true,48\n"
            "2,BOS,101,Player 101,true,30\n"
            "2,BOS,102,Player 102,true,10\n"
            "2,NYK,202,Player 202,true,48\n"
            "3,BOS,101,Player 101,true,30\n"
            "3,BOS,102,Player 102,true,10\n"
            "3,LAL,203,Player 203,true,48\n"
        ),
        encoding="utf-8",
    )

    report = run_rawr(
        games_csv,
        game_players_csv,
        min_games=1,
        ridge_alpha=1.0,
        teams=["BOS"],
        min_average_minutes=20.0,
        min_total_minutes=40.0,
    )

    assert "Player 101" in report
    assert "Player 102" not in report
    assert "Player 201" in report
    assert "Player 202" in report
    assert "Player 203" in report


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
    (normalized_games_dir / "MIL_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,MIL,BOS,false,-2,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "NYK_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "2,2023-24,2024-04-03,NYK,BOS,true,2,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "LAL_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "3,2023-24,2024-04-05,LAL,BOS,false,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,48\n"
            "2,BOS,102,Player 102,true,48\n"
            "3,BOS,101,Player 101,true,24\n"
            "3,BOS,102,Player 102,true,24\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "MIL_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,MIL,201,Player 201,true,48\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "NYK_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "2,NYK,202,Player 202,true,48\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "LAL_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "3,LAL,201,Player 201,true,24\n"
            "3,LAL,202,Player 202,true,24\n"
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
            "--player-metrics-db-path",
            str(tmp_path / "app" / "player_metrics.sqlite3"),
            "--source-data-dir",
            str(tmp_path / "source"),
            "--min-games",
            "1",
            "--ridge-alpha",
            "1.0",
            "--min-average-minutes",
            "0",
            "--min-total-minutes",
            "0",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "RAWR results (Game-level player model)" in captured.out
    assert "RAWR CLI" in captured.err
    assert not (tmp_path / "combined" / "games.csv").exists()
    assert not (tmp_path / "combined" / "game_players.csv").exists()


def test_main_filters_cached_scope_by_team_and_season(
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
    (normalized_games_dir / "MIL_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,MIL,BOS,false,-2,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "NYK_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "2,2023-24,2024-04-03,NYK,BOS,true,2,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "LAL_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "3,2023-24,2024-04-05,LAL,BOS,false,0,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (normalized_games_dir / "NYK_2024-25.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "4,2024-25,2025-01-01,NYK,BOS,true,20,Regular Season,nba_api\n"
            "4,2024-25,2025-01-01,BOS,NYK,false,-20,Regular Season,nba_api\n"
            "5,2024-25,2025-01-03,NYK,MIL,false,15,Regular Season,nba_api\n"
            "5,2024-25,2025-01-03,MIL,NYK,true,-15,Regular Season,nba_api\n"
            "6,2024-25,2025-01-05,NYK,CLE,true,10,Regular Season,nba_api\n"
            "6,2024-25,2025-01-05,CLE,NYK,false,-10,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,48\n"
            "2,BOS,102,Player 102,true,48\n"
            "3,BOS,101,Player 101,true,24\n"
            "3,BOS,102,Player 102,true,24\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "MIL_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,MIL,201,Player 201,true,48\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "NYK_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "2,NYK,202,Player 202,true,48\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "LAL_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "3,LAL,201,Player 201,true,24\n"
            "3,LAL,202,Player 202,true,24\n"
        ),
        encoding="utf-8",
    )
    (normalized_players_dir / "NYK_2024-25.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "4,NYK,201,Player 201,true,48\n"
            "4,BOS,101,Player 101,true,48\n"
            "5,NYK,201,Player 201,true,48\n"
            "5,MIL,301,Player 301,true,48\n"
            "6,NYK,202,Player 202,true,48\n"
            "6,CLE,302,Player 302,true,48\n"
        ),
        encoding="utf-8",
    )

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
            "--combined-games-csv",
            str(tmp_path / "combined" / "games.csv"),
            "--combined-game-players-csv",
            str(tmp_path / "combined" / "game_players.csv"),
            "--player-metrics-db-path",
            str(tmp_path / "app" / "player_metrics.sqlite3"),
            "--source-data-dir",
            str(tmp_path / "source"),
            "--min-games",
            "1",
            "--ridge-alpha",
            "1.0",
            "--min-average-minutes",
            "0",
            "--min-total-minutes",
            "0",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Player 101" in captured.out
    assert not (tmp_path / "combined" / "games.csv").exists()
    assert not (tmp_path / "combined" / "game_players.csv").exists()


def test_rawr_cli_accepts_game_count_shrinkage_mode(monkeypatch):
    captured_args: list[object] = []

    monkeypatch.setattr(
        "wowy.apps.rawr.cli.prepare_and_run_rawr",
        lambda args: captured_args.append(args) or "ok",
    )

    exit_code = main(
        [
            "--season",
            "2023-24",
            "--shrinkage-mode",
            "game-count",
            "--shrinkage-strength",
            "0.5",
        ]
    )

    assert exit_code == 0
    assert captured_args[0].shrinkage_mode == "game-count"
    assert captured_args[0].shrinkage_strength == 0.5


def test_rawr_cli_accepts_minute_shrinkage_mode(monkeypatch):
    captured_args: list[object] = []

    monkeypatch.setattr(
        "wowy.apps.rawr.cli.prepare_and_run_rawr",
        lambda args: captured_args.append(args) or "ok",
    )

    exit_code = main(
        [
            "--season",
            "2023-24",
            "--shrinkage-mode",
            "minutes",
            "--shrinkage-strength",
            "0.5",
            "--shrinkage-minute-scale",
            "24",
        ]
    )

    assert exit_code == 0
    assert captured_args[0].shrinkage_mode == "minutes"
    assert captured_args[0].shrinkage_strength == 0.5
    assert captured_args[0].shrinkage_minute_scale == 24.0


def test_main_rejects_negative_filters():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--min-games", "-1"])


def test_main_rejects_negative_ridge_alpha():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--ridge-alpha", "-1"])


def test_main_rejects_negative_top_n():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--top-n", "-1"])


def test_main_rejects_negative_min_average_minutes():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--min-average-minutes", "-1"])


def test_main_rejects_negative_min_total_minutes():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--min-total-minutes", "-1"])


def test_parse_ridge_grid_rejects_invalid_values():
    with pytest.raises(ValueError, match="non-negative"):
        parse_ridge_grid("1,-3")
