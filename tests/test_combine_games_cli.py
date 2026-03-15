from __future__ import annotations

import csv
from pathlib import Path

import pytest

from wowy.data.combine import (
    combine_csv_paths,
    combine_csvs,
    combine_normalized_data,
    combine_normalized_files,
)
from wowy.data.combine_cli import main
from wowy.normalized_io import (
    NORMALIZED_GAME_PLAYERS_HEADER,
    NORMALIZED_GAMES_HEADER,
)


def test_combine_csvs_writes_combined_output(tmp_path: Path):
    input_dir = tmp_path / "games"
    input_dir.mkdir()
    (input_dir / "BOS.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
            "2,2023-24,2024-04-03,BOS,NYK,false,-5,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    (input_dir / "LAL.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "3,2023-24,2024-04-04,LAL,DEN,true,7,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "combined" / "games.csv"
    combine_csvs(input_dir, output_path, NORMALIZED_GAMES_HEADER)

    with open(output_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows == [
        NORMALIZED_GAMES_HEADER,
        ["1", "2023-24", "2024-04-01", "BOS", "MIL", "true", "10", "Regular Season", "nba_api"],
        ["2", "2023-24", "2024-04-03", "BOS", "NYK", "false", "-5", "Regular Season", "nba_api"],
        ["3", "2023-24", "2024-04-04", "LAL", "DEN", "true", "7", "Regular Season", "nba_api"],
    ]
    assert not list((tmp_path / "combined").glob("*.tmp-*"))


def test_combine_csvs_rejects_empty_input_dir(tmp_path: Path):
    input_dir = tmp_path / "empty"
    input_dir.mkdir()

    with pytest.raises(ValueError, match="No CSV files found"):
        combine_csvs(input_dir, tmp_path / "combined.csv", NORMALIZED_GAMES_HEADER)


def test_combine_csvs_rejects_unexpected_header(tmp_path: Path):
    input_dir = tmp_path / "games"
    input_dir.mkdir()
    bad_csv = input_dir / "bad.csv"
    bad_csv.write_text(
        "game_id,team,margin\n1,BOS,10\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unexpected CSV header"):
        combine_csvs(input_dir, tmp_path / "combined.csv", NORMALIZED_GAMES_HEADER)


def test_combine_csv_paths_writes_only_selected_files(tmp_path: Path):
    included_csv = tmp_path / "BOS.csv"
    included_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    excluded_csv = tmp_path / "LAL.csv"
    excluded_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "2,2022-23,2023-04-01,LAL,DEN,true,7,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "combined" / "games.csv"
    combine_csv_paths([included_csv], output_path, NORMALIZED_GAMES_HEADER)

    with open(output_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows == [
        NORMALIZED_GAMES_HEADER,
        ["1", "2023-24", "2024-04-01", "BOS", "MIL", "true", "10", "Regular Season", "nba_api"],
    ]


def test_combine_csv_paths_rejects_empty_input_list(tmp_path: Path):
    with pytest.raises(ValueError, match="No input CSV files provided"):
        combine_csv_paths([], tmp_path / "combined.csv", NORMALIZED_GAMES_HEADER)


def test_combine_normalized_data_writes_both_outputs(tmp_path: Path):
    games_input_dir = tmp_path / "games"
    games_input_dir.mkdir()
    (games_input_dir / "BOS.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_input_dir = tmp_path / "game_players"
    game_players_input_dir.mkdir()
    (game_players_input_dir / "BOS.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,\n"
        ),
        encoding="utf-8",
    )

    games_output = tmp_path / "combined" / "games.csv"
    game_players_output = tmp_path / "combined" / "game_players.csv"
    combine_normalized_data(
        games_input_dir=games_input_dir,
        game_players_input_dir=game_players_input_dir,
        games_output_path=games_output,
        game_players_output_path=game_players_output,
    )

    with open(games_output, "r", encoding="utf-8", newline="") as f:
        game_rows = list(csv.reader(f))
    with open(game_players_output, "r", encoding="utf-8", newline="") as f:
        player_rows = list(csv.reader(f))

    assert game_rows == [
        NORMALIZED_GAMES_HEADER,
        ["1", "2023-24", "2024-04-01", "BOS", "MIL", "true", "10", "Regular Season", "nba_api"],
    ]
    assert player_rows == [
        NORMALIZED_GAME_PLAYERS_HEADER,
        ["1", "BOS", "101", "Player 101", "true", ""],
    ]
    assert not list((tmp_path / "combined").glob("*.tmp-*"))


def test_combine_normalized_files_writes_only_requested_outputs(tmp_path: Path):
    included_games_csv = tmp_path / "BOS_games.csv"
    included_games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    excluded_games_csv = tmp_path / "LAL_games.csv"
    excluded_games_csv.write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "2,2022-23,2023-04-01,LAL,DEN,true,7,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    included_players_csv = tmp_path / "BOS_players.csv"
    included_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,\n"
        ),
        encoding="utf-8",
    )
    excluded_players_csv = tmp_path / "LAL_players.csv"
    excluded_players_csv.write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "2,LAL,202,Player 202,true,\n"
        ),
        encoding="utf-8",
    )

    games_output = tmp_path / "combined" / "games.csv"
    game_players_output = tmp_path / "combined" / "game_players.csv"
    combine_normalized_files(
        games_input_paths=[included_games_csv],
        game_players_input_paths=[included_players_csv],
        games_output_path=games_output,
        game_players_output_path=game_players_output,
    )

    with open(games_output, "r", encoding="utf-8", newline="") as f:
        game_rows = list(csv.reader(f))
    with open(game_players_output, "r", encoding="utf-8", newline="") as f:
        player_rows = list(csv.reader(f))

    assert game_rows == [
        NORMALIZED_GAMES_HEADER,
        ["1", "2023-24", "2024-04-01", "BOS", "MIL", "true", "10", "Regular Season", "nba_api"],
    ]
    assert player_rows == [
        NORMALIZED_GAME_PLAYERS_HEADER,
        ["1", "BOS", "101", "Player 101", "true", ""],
    ]


def test_main_combines_files(tmp_path: Path):
    games_input_dir = tmp_path / "games"
    games_input_dir.mkdir()
    (games_input_dir / "BOS.csv").write_text(
        (
            "game_id,season,game_date,team,opponent,is_home,margin,season_type,source\n"
            "1,2023-24,2024-04-01,BOS,MIL,true,10,Regular Season,nba_api\n"
        ),
        encoding="utf-8",
    )
    game_players_input_dir = tmp_path / "game_players"
    game_players_input_dir.mkdir()
    (game_players_input_dir / "BOS.csv").write_text(
        (
            "game_id,team,player_id,player_name,appeared,minutes\n"
            "1,BOS,101,Player 101,true,\n"
        ),
        encoding="utf-8",
    )

    games_output = tmp_path / "combined" / "games.csv"
    game_players_output = tmp_path / "combined" / "game_players.csv"
    exit_code = main(
        [
            "--games-input-dir",
            str(games_input_dir),
            "--game-players-input-dir",
            str(game_players_input_dir),
            "--games-output",
            str(games_output),
            "--game-players-output",
            str(game_players_output),
        ]
    )

    assert exit_code == 0
    assert games_output.exists()
    assert game_players_output.exists()
