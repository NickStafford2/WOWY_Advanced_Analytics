from __future__ import annotations

from pathlib import Path

import pytest

from wowy.main import main


def test_main_runs_with_temp_csv(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    write_games_csv,
):
    csv_path = tmp_path / "games.csv"
    write_games_csv(
        csv_path,
        [
            ["1", "team_1", "10", "player_A;player_B;player_C"],
            ["2", "team_1", "0", "player_B;player_C;player_D"],
            ["3", "team_1", "-10", "player_C;player_D;player_E"],
        ],
    )

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


def test_help_works(capsys: pytest.CaptureFixture[str]):
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "Compute a simple game-level WOWY score from a CSV file." in captured.out
