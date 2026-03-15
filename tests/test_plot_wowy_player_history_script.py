from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path

import pytest


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "plot_wowy_player_history.py"
)
SPEC = importlib.util.spec_from_file_location("plot_wowy_player_history", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def write_player_season_csv(path: Path, rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "season",
                "player_id",
                "player_name",
                "games_with",
                "games_without",
                "avg_margin_with",
                "avg_margin_without",
                "wowy_score",
                "average_minutes",
                "total_minutes",
            ]
        )
        writer.writerows(rows)


def test_select_top_players_ranks_by_average_wowy() -> None:
    points = [
        MODULE.PlayerSeasonPoint("2020-21", 101, "Player A", 5.0, 10, 10, 30.0, 600.0),
        MODULE.PlayerSeasonPoint("2021-22", 101, "Player A", 7.0, 10, 10, 31.0, 620.0),
        MODULE.PlayerSeasonPoint("2020-21", 102, "Player B", 8.0, 10, 10, 30.0, 600.0),
        MODULE.PlayerSeasonPoint("2021-22", 102, "Player B", 2.0, 10, 10, 31.0, 620.0),
        MODULE.PlayerSeasonPoint("2020-21", 103, "Player C", 9.0, 10, 10, 30.0, 600.0),
    ]

    selected = MODULE.select_top_players(points, top_n=2, min_seasons=2)

    assert selected == [101, 102]


def test_main_writes_png_for_selected_span(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "player_seasons.csv"
    output_path = tmp_path / "charts" / "history.png"
    write_player_season_csv(
        input_path,
        [
            ["2020-21", 101, "Player A", 10, 10, 1.0, -1.0, 5.0, 30.0, 600.0],
            ["2021-22", 101, "Player A", 10, 10, 2.0, -1.0, 7.0, 31.0, 620.0],
            ["2020-21", 102, "Player B", 10, 10, 1.0, -1.0, 8.0, 30.0, 600.0],
            ["2021-22", 102, "Player B", 10, 10, 1.0, -1.0, 2.0, 31.0, 620.0],
            ["2022-23", 103, "Player C", 10, 10, 1.0, -1.0, 9.0, 32.0, 640.0],
        ],
    )

    exit_code = MODULE.main(
        [
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--season",
            "2020-21",
            "--season",
            "2021-22",
            "--top-n",
            "2",
            "--min-seasons",
            "2",
            "--title",
            "Test WOWY History",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert output_path.exists()
    assert output_path.stat().st_size > 0
    assert "wrote player history chart" in captured.out


def test_run_exits_cleanly_on_keyboard_interrupt(
    capsys: pytest.CaptureFixture[str],
    monkeypatch,
) -> None:
    monkeypatch.setattr(MODULE, "main", lambda argv=None: (_ for _ in ()).throw(KeyboardInterrupt))

    exit_code = MODULE.run([])
    captured = capsys.readouterr()

    assert exit_code == 130
    assert "Interrupted. Shutting down cleanly." in captured.err
