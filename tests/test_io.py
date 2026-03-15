from __future__ import annotations

from pathlib import Path

import pytest

from wowy.io import load_games_from_csv


def test_load_games_from_csv(tmp_path: Path, write_games_csv):
    csv_path = tmp_path / "games.csv"
    write_games_csv(
        csv_path,
        [
            ["1", "team_1", "10", "101;102;103"],
            ["2", "team_1", "-5", "102;103;104"],
        ],
    )

    games = load_games_from_csv(csv_path)

    assert len(games) == 2
    assert games[0]["game_id"] == "1"
    assert games[0]["team"] == "team_1"
    assert games[0]["margin"] == 10.0
    assert games[0]["players"] == {101, 102, 103}


def test_load_games_from_csv_missing_column(tmp_path: Path):
    csv_path = tmp_path / "bad_games.csv"
    csv_path.write_text("game_id,team,margin\n1,team_1,10\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required CSV columns"):
        load_games_from_csv(csv_path)


def test_load_games_from_csv_invalid_margin(tmp_path: Path, write_games_csv):
    csv_path = tmp_path / "bad_games.csv"
    write_games_csv(
        csv_path,
        [["1", "team_1", "not_a_number", "101;102"]],
    )

    with pytest.raises(ValueError, match="Invalid margin"):
        load_games_from_csv(csv_path)


def test_load_games_from_csv_empty_players(tmp_path: Path, write_games_csv):
    csv_path = tmp_path / "bad_games.csv"
    write_games_csv(
        csv_path,
        [["1", "team_1", "10", "   "]],
    )

    with pytest.raises(ValueError, match="has no players listed"):
        load_games_from_csv(csv_path)


def test_load_games_from_csv_invalid_player_id(tmp_path: Path, write_games_csv):
    csv_path = tmp_path / "bad_games.csv"
    write_games_csv(
        csv_path,
        [["1", "team_1", "10", "101;not_a_player_id"]],
    )

    with pytest.raises(ValueError, match="Invalid player id"):
        load_games_from_csv(csv_path)
