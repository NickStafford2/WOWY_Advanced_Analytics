from __future__ import annotations

from pathlib import Path

import pytest

from wowy.cli import build_wowy_report, run_wowy
from wowy.main import main
from wowy.types import WowyGameRecord


def test_main_runs_with_temp_csv(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch,
    write_games_csv,
):
    csv_path = tmp_path / "games.csv"
    write_games_csv(
        csv_path,
        [
            ["1", "2023-24", "team_1", "10", "101;102;103"],
            ["2", "2023-24", "team_1", "0", "102;103;104"],
            ["3", "2023-24", "team_1", "-10", "103;104;105"],
        ],
    )

    monkeypatch.setattr("wowy.cli.load_player_names_from_cache", lambda _: {})

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
    assert "101" in captured.out


def test_run_wowy_returns_report_text(tmp_path: Path, write_games_csv):
    csv_path = tmp_path / "games.csv"
    write_games_csv(
        csv_path,
        [
            ["1", "2023-24", "team_1", "10", "101;102;103"],
            ["2", "2023-24", "team_1", "0", "102;103;104"],
            ["3", "2023-24", "team_1", "-10", "103;104;105"],
        ],
    )

    report = run_wowy(
        csv_path,
        min_games_with=1,
        min_games_without=1,
        player_names={101: "Player 101"},
    )

    assert "WOWY results (Version 1)" in report
    assert "Player 101" in report


def test_run_wowy_applies_top_n(tmp_path: Path, write_games_csv):
    csv_path = tmp_path / "games.csv"
    write_games_csv(
        csv_path,
        [
            ["1", "2023-24", "team_1", "10", "101;102"],
            ["2", "2023-24", "team_1", "0", "101"],
            ["3", "2023-24", "team_1", "-10", "102"],
        ],
    )

    report = run_wowy(
        csv_path,
        min_games_with=1,
        min_games_without=1,
        player_names={101: "Player 101", 102: "Player 102"},
        top_n=1,
    )

    assert "Player 101" in report
    assert "Player 102" not in report


def test_main_rejects_negative_filters():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--min-games-with", "-1"])


def test_main_rejects_negative_top_n():
    with pytest.raises(ValueError, match="non-negative"):
        main(["--top-n", "-1"])


def test_build_wowy_report_formats_preloaded_games():
    games: list[WowyGameRecord] = [
        WowyGameRecord("1", "2023-24", "team_1", 10.0, {101, 102, 103}),
        WowyGameRecord("2", "2023-24", "team_1", -5.0, {102, 103, 104}),
    ]

    report = build_wowy_report(
        games,
        min_games_with=1,
        min_games_without=1,
        player_names={101: "Player 101"},
    )

    assert "WOWY results (Version 1)" in report
    assert "Player 101" in report


def test_help_works(capsys: pytest.CaptureFixture[str]):
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "Compute a simple game-level WOWY score from a CSV file." in captured.out
