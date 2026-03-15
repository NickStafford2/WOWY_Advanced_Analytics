from __future__ import annotations

import csv
from pathlib import Path

import pytest

from wowy.combine_games_cli import combine_game_csvs, main


def test_combine_game_csvs_writes_combined_output(
    tmp_path: Path,
    write_games_csv,
):
    input_dir = tmp_path / "team_games"
    input_dir.mkdir()
    write_games_csv(
        input_dir / "BOS.csv",
        [["1", "BOS", "10", "101;102"], ["2", "BOS", "-5", "101;103"]],
    )
    write_games_csv(
        input_dir / "LAL.csv",
        [["3", "LAL", "7", "201;202"]],
    )

    output_path = tmp_path / "combined" / "games.csv"
    combine_game_csvs(input_dir, output_path)

    with open(output_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows == [
        ["game_id", "team", "margin", "players"],
        ["1", "BOS", "10", "101;102"],
        ["2", "BOS", "-5", "101;103"],
        ["3", "LAL", "7", "201;202"],
    ]


def test_combine_game_csvs_rejects_empty_input_dir(tmp_path: Path):
    input_dir = tmp_path / "empty"
    input_dir.mkdir()

    with pytest.raises(ValueError, match="No CSV files found"):
        combine_game_csvs(input_dir, tmp_path / "combined.csv")


def test_combine_game_csvs_rejects_unexpected_header(tmp_path: Path):
    input_dir = tmp_path / "team_games"
    input_dir.mkdir()
    bad_csv = input_dir / "bad.csv"
    bad_csv.write_text(
        "game_id,team,margin\n1,BOS,10\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unexpected CSV header"):
        combine_game_csvs(input_dir, tmp_path / "combined.csv")


def test_main_combines_files(tmp_path: Path, write_games_csv):
    input_dir = tmp_path / "team_games"
    input_dir.mkdir()
    write_games_csv(
        input_dir / "BOS.csv",
        [["1", "BOS", "10", "101;102"]],
    )

    output_path = tmp_path / "combined" / "games.csv"
    exit_code = main(["--input-dir", str(input_dir), "--output", str(output_path)])

    assert exit_code == 0
    assert output_path.exists()
