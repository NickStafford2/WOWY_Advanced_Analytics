from __future__ import annotations

import csv
import tempfile
from pathlib import Path

import pytest

from wowy.main import (
    GameRecord,
    PlayerStats,
    compute_wowy,
    filter_results,
    format_results_table,
    load_games_from_csv,
    main,
)


def test_compute_wowy_basic():
    games: list[GameRecord] = [
        {
            "game_id": "1",
            "team": "team_1",
            "margin": 10.0,
            "players": {"player_A", "player_B", "player_C"},
        },
        {
            "game_id": "2",
            "team": "team_1",
            "margin": 0.0,
            "players": {"player_B", "player_C", "player_D"},
        },
        {
            "game_id": "3",
            "team": "team_1",
            "margin": -10.0,
            "players": {"player_C", "player_D", "player_E"},
        },
    ]

    results = compute_wowy(games)

    assert results["player_A"]["games_with"] == 1
    assert results["player_A"]["games_without"] == 2
    assert results["player_A"]["avg_margin_with"] == 10.0
    assert results["player_A"]["avg_margin_without"] == -5.0
    assert results["player_A"]["wowy_score"] == 15.0


def test_filter_results():
    results: dict[str, PlayerStats] = {
        "player_A": {
            "games_with": 3,
            "games_without": 3,
            "avg_margin_with": 5.0,
            "avg_margin_without": 1.0,
            "wowy_score": 4.0,
        },
        "player_B": {
            "games_with": 1,
            "games_without": 5,
            "avg_margin_with": 2.0,
            "avg_margin_without": 0.0,
            "wowy_score": 2.0,
        },
        "player_C": {
            "games_with": 4,
            "games_without": 1,
            "avg_margin_with": 1.0,
            "avg_margin_without": -1.0,
            "wowy_score": 2.0,
        },
    }

    filtered = filter_results(results, min_games_with=2, min_games_without=2)

    assert "player_A" in filtered
    assert "player_B" not in filtered
    assert "player_C" not in filtered


def test_load_games_from_csv():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "games.csv"

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["game_id", "team", "margin", "players"])
            writer.writerow(["1", "team_1", "10", "player_A;player_B;player_C"])
            writer.writerow(["2", "team_1", "-5", "player_B;player_C;player_D"])

        games = load_games_from_csv(csv_path)

        assert len(games) == 2
        assert games[0]["game_id"] == "1"
        assert games[0]["team"] == "team_1"
        assert games[0]["margin"] == 10.0
        assert games[0]["players"] == {"player_A", "player_B", "player_C"}


def test_load_games_from_csv_missing_column():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "bad_games.csv"

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["game_id", "team", "margin"])
            writer.writerow(["1", "team_1", "10"])

        with pytest.raises(ValueError, match="Missing required CSV columns"):
            load_games_from_csv(csv_path)


def test_load_games_from_csv_invalid_margin():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "bad_games.csv"

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["game_id", "team", "margin", "players"])
            writer.writerow(["1", "team_1", "not_a_number", "player_A;player_B"])

        with pytest.raises(ValueError, match="Invalid margin"):
            load_games_from_csv(csv_path)


def test_load_games_from_csv_empty_players():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "bad_games.csv"

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["game_id", "team", "margin", "players"])
            writer.writerow(["1", "team_1", "10", "   "])

        with pytest.raises(ValueError, match="has no players listed"):
            load_games_from_csv(csv_path)


def test_format_results_table_contains_expected_text():
    results: dict[str, PlayerStats] = {
        "player_A": {
            "games_with": 3,
            "games_without": 2,
            "avg_margin_with": 5.0,
            "avg_margin_without": 1.0,
            "wowy_score": 4.0,
        }
    }

    output = format_results_table(results)

    assert "WOWY results (Version 1)" in output
    assert "player_A" in output
    assert "4.00" in output


def test_main_runs_with_temp_csv(capsys):
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "games.csv"

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["game_id", "team", "margin", "players"])
            writer.writerow(["1", "team_1", "10", "player_A;player_B;player_C"])
            writer.writerow(["2", "team_1", "0", "player_B;player_C;player_D"])
            writer.writerow(["3", "team_1", "-10", "player_C;player_D;player_E"])

        exit_code = main(
            [
                "--csv",
                str(csv_path),
                "--min-games-with",
                "1",
                "--min-games-without",
                "1",
            ]
        )

        captured = capsys.readouterr()

        assert exit_code == 0
        assert "WOWY results (Version 1)" in captured.out
        assert "player_A" in captured.out


def test_main_rejects_negative_filters():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--min-games-with", "-1"])


def test_help_works(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "Compute a simple game-level WOWY score from a CSV file." in captured.out
