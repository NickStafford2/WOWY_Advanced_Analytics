from pathlib import Path
import csv
import tempfile

from src.main import compute_wowy, filter_results, load_games_from_csv


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

        try:
            load_games_from_csv(csv_path)
            assert False, "Expected ValueError for missing players column"
        except ValueError as exc:
            assert "Missing required CSV columns" in str(exc)
