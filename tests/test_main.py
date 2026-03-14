from pathlib import Path
import csv
import tempfile

import pytest

from src.main import (
    build_parser,
    compute_wowy,
    filter_results,
    format_results_table,
    load_games_from_csv,
    main,
    parse_players,
)


def test_parse_players_trims_and_deduplicates():
    assert parse_players(" player_A ; player_B;player_A ; ; ") == {
        "player_A",
        "player_B",
    }


def test_compute_wowy_basic():
    games = [
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
    results = {
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


def test_load_games_from_csv_rejects_empty_players():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "bad_games.csv"

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["game_id", "team", "margin", "players"])
            writer.writerow(["1", "team_1", "10", "   "])

        with pytest.raises(
            ValueError, match="players column must contain at least one player"
        ):
            load_games_from_csv(csv_path)


def test_load_games_from_csv_rejects_non_numeric_margin():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "bad_games.csv"

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["game_id", "team", "margin", "players"])
            writer.writerow(["1", "team_1", "abc", "player_A;player_B"])

        with pytest.raises(ValueError, match="margin must be numeric"):
            load_games_from_csv(csv_path)


def test_format_results_table_handles_empty_results():
    output = format_results_table({})
    assert "No players matched the current filtering rules." in output


def test_main_returns_zero_and_prints_results(capsys, tmp_path):
    csv_path = tmp_path / "games.csv"
    csv_path.write_text(
        "game_id,team,margin,players\n"
        '1,team_1,10,"player_A;player_B;player_C"\n'
        '2,team_1,0,"player_B;player_C;player_D"\n'
        '3,team_1,-10,"player_C;player_D;player_E"\n',
        encoding="utf-8",
    )

    rc = main([str(csv_path), "--min-games-with", "1", "--min-games-without", "1"])

    assert rc == 0
    captured = capsys.readouterr()
    assert "WOWY results (Version 1)" in captured.out
    assert "player_A" in captured.out


def test_build_parser_supports_help_flag(capsys):
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--help"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "Compute a simple game-level WOWY score" in captured.out
