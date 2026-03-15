from __future__ import annotations

import csv
from pathlib import Path

import pytest

from wowy.rebuild_wowy_cache_cli import main, rebuild_wowy_cache


def test_rebuild_wowy_cache_rewrites_team_game_files(tmp_path: Path):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,8,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,35.0\n"
            "1,BOS,102,Player 102,false,0.0\n"
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "team_games"
    rebuilt = rebuild_wowy_cache(
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=output_dir,
    )

    assert rebuilt == 1
    with open(output_dir / "BOS_2023-24.csv", "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows == [
        ["game_id", "season", "team", "margin", "players"],
        ["1", "2023-24", "BOS", "8.0", "101"],
    ]


def test_rebuild_wowy_cache_removes_stale_derived_files(tmp_path: Path):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,8,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,35.0\n"
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "team_games"
    output_dir.mkdir()
    (output_dir / "OLD_2019-20.csv").write_text(
        "game_id,team,margin,players\n1,BOS,8,101\n",
        encoding="utf-8",
    )

    rebuilt = rebuild_wowy_cache(
        normalized_games_input_dir=normalized_games_dir,
        normalized_game_players_input_dir=normalized_players_dir,
        wowy_output_dir=output_dir,
    )

    assert rebuilt == 1
    assert not (output_dir / "OLD_2019-20.csv").exists()
    assert (output_dir / "BOS_2023-24.csv").exists()


def test_rebuild_wowy_cache_rejects_missing_matching_player_file(tmp_path: Path):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n",
        encoding="utf-8",
    )

    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()

    with pytest.raises(ValueError, match="Missing normalized game-player CSV"):
        rebuild_wowy_cache(
            normalized_games_input_dir=normalized_games_dir,
            normalized_game_players_input_dir=normalized_players_dir,
            wowy_output_dir=tmp_path / "team_games",
        )


def test_main_runs_rebuild(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    normalized_games_dir = tmp_path / "normalized_games"
    normalized_games_dir.mkdir()
    (normalized_games_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,8,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    normalized_players_dir = tmp_path / "normalized_game_players"
    normalized_players_dir.mkdir()
    (normalized_players_dir / "BOS_2023-24.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,35.0\n"
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--normalized-games-input-dir",
            str(normalized_games_dir),
            "--normalized-game-players-input-dir",
            str(normalized_players_dir),
            "--wowy-output-dir",
            str(tmp_path / "team_games"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Rebuilt 1 WOWY team-game CSV files." in captured.out
